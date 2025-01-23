import { ActionDefinition, ActionContext, OutputObject } from 'connery';
import pkg from 'pg';
const { Client } = pkg;
import { Anthropic } from '@anthropic-ai/sdk';

const actionDefinition: ActionDefinition = {
  key: 'chatWithYourDb',
  name: 'Chat with your DB',
  description: 'Users can send DB requests in natural language and receive data and/or helpful feedback.',
  type: 'read',
  inputParameters: [
    {
      key: 'anthropicApiKey',
      name: 'Anthropic API Key',
      description: 'Your Anthropic API key',
      type: 'string',
      validation: {
        required: true,
      },
    },
    {
      key: 'connectionString',
      name: 'Database Connection String',
      description: 'PostgreSQL connection string (should use read-only credentials)',
      type: 'string',
      validation: {
        required: true,
      },
    },
    {
      key: 'instructions',
      name: 'Instructions',
      description: 'Optional instructions for processing the response',
      type: 'string',
      validation: {
        required: false,
      },
    },
    {
      key: 'maxRows',
      name: 'Maximum Rows',
      description: 'Maximum number of rows to return (default: 100)',
      type: 'string',
      validation: {
        required: false,
      },
    },
    {
      key: 'question',
      name: 'Question',
      description: 'Your database question in natural language',
      type: 'string',
      validation: {
        required: true,
      },
    },
  ],
  operation: {
    handler: handler,
  },
  outputParameters: [
    {
      key: 'data',
      name: 'Data',
      description: 'The data returned by your database query',
      type: 'string',
      validation: {
        required: true,
      },
    },
    {
      key: 'query',
      name: 'Query',
      description: 'The generated SQL query',
      type: 'string',
      validation: {
        required: true,
      },
    },
  ],
};

export default actionDefinition;

export async function handler({ input }: ActionContext): Promise<OutputObject> {
  let client: pkg.Client | null = null;

  try {
    // Always generate new schema
    client = new Client(input.connectionString);
    await client.connect();
    await client.query('SELECT 1'); // Test connection
    const schemaInfo = await getSchemaInfo(client);

    const sqlQuery = await generateSqlQuery(input.anthropicApiKey, schemaInfo, input.question, parseInt(input.maxRows || '100'));
    const result = await client.query(sqlQuery);
    
    // Format each part separately
    const dataResponse = formatDataResponse(result.rows, input.instructions);
    const queryResponse = formatQueryResponse(sqlQuery);

    // Return all responses
    return {
      data: dataResponse,
      query: queryResponse,
    };
  } catch (error: unknown) {
    throw error;
  } finally {
    if (client) {
      try {
        await client.end();
      } catch (closeError) {
        // Silently handle connection closing errors
      }
    }
  }
}

async function getSchemaInfo(client: pkg.Client): Promise<string> {
  const schemaQuery = `
    WITH RECURSIVE table_info AS (
      SELECT 
        t.table_schema,
        t.table_name,
        t.table_type,
        (
          SELECT jsonb_agg(jsonb_build_object(
            'column_name', c.column_name,
            'data_type', CASE 
              WHEN c.data_type = 'USER-DEFINED' THEN c.udt_name 
              ELSE c.data_type 
            END,
            'is_nullable', c.is_nullable,
            'column_default', c.column_default
          ) ORDER BY c.ordinal_position)
          FROM information_schema.columns c 
          WHERE c.table_schema = t.table_schema 
          AND c.table_name = t.table_name
        ) as columns,
        (
          SELECT jsonb_agg(jsonb_build_object(
            'constraint_type', tc.constraint_type,
            'column_name', kcu.column_name,
            'foreign_table', CASE 
              WHEN tc.constraint_type = 'FOREIGN KEY' 
              THEN ccu.table_name 
              ELSE null 
            END,
            'foreign_column', CASE 
              WHEN tc.constraint_type = 'FOREIGN KEY' 
              THEN ccu.column_name 
              ELSE null 
            END
          ))
          FROM information_schema.table_constraints tc
          JOIN information_schema.key_column_usage kcu 
            ON tc.constraint_name = kcu.constraint_name
          LEFT JOIN information_schema.constraint_column_usage ccu 
            ON tc.constraint_name = ccu.constraint_name
          WHERE tc.table_schema = t.table_schema 
          AND tc.table_name = t.table_name
          AND tc.constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE')
        ) as constraints
      FROM information_schema.tables t
      WHERE t.table_schema NOT IN ('pg_catalog', 'information_schema')
      AND t.table_type IN ('BASE TABLE', 'VIEW', 'MATERIALIZED VIEW')
    )
    SELECT jsonb_pretty(
      jsonb_agg(
        jsonb_build_object(
          'schema', table_schema,
          'name', table_name,
          'type', table_type,
          'columns', columns,
          'constraints', constraints
        )
        ORDER BY table_schema, table_name
      )
    ) as schema_json
    FROM table_info
    WHERE columns IS NOT NULL
    LIMIT 1000;  -- Safety limit for very large DBs
  `;

  try {
    const schemaResult = await Promise.race([
      client.query(schemaQuery),
      new Promise((_, reject) => 
        setTimeout(() => reject(new Error('Schema query timeout after 30s')), 30000)
      )
    ]) as pkg.QueryResult;
    
    return schemaResult.rows[0].schema_json || '[]';
  } catch (error) {
    // Fallback to a simpler schema query if the detailed one fails
    const simpleSchemaQuery = `
      SELECT jsonb_pretty(jsonb_agg(
        jsonb_build_object(
          'name', table_name,
          'columns', (
            SELECT jsonb_agg(jsonb_build_object(
              'column_name', column_name,
              'data_type', data_type
            ))
            FROM information_schema.columns c 
            WHERE c.table_name = t.table_name
          )
        )
      ))
      FROM information_schema.tables t
      WHERE table_schema = 'public'
      AND table_type = 'BASE TABLE';
    `;
    
    const fallbackResult = await client.query(simpleSchemaQuery);
    return fallbackResult.rows[0].jsonb_pretty || '[]';
  }
}

async function generateSqlQuery(apiKey: string, schemaInfo: string, question: string, maxRows: number): Promise<string> {
  const systemPrompt = `You are a PostgreSQL expert. Generate secure, read-only SQL queries based on natural language questions.
        Schema information: ${schemaInfo}

        CRITICAL: All patterns below are EXAMPLES. You must adapt them to use actual table and column names from the provided schema information. Never use example table names in actual queries!

        RESPONSE FORMAT:
        - Return ONLY the raw SQL query
        - NO explanations
        - NO markdown
        - NO comments
        - NO prefixes
        - ONLY SQL code

        CRITICAL RULES:
        1. NEVER use window functions inside GROUP BY
        2. NEVER use aggregates inside GROUP BY
        3. NEVER put ORDER BY inside CTEs with UNION
        4. ALWAYS use result_type column for sorting with UNION + totals
        5. ALWAYS calculate raw values before aggregating
        6. ALWAYS use explicit CAST for numeric calculations
        7. ALWAYS handle NULL values with NULLIF in divisions
        8. ALWAYS verify table/column existence in schema
        9. ALWAYS use proper table aliases matching schema
        10. ALWAYS qualify all column names with table aliases
        11. ALWAYS use schema-appropriate JOIN conditions
        12. ALWAYS limit results using: LIMIT ${maxRows}

        PATTERN TEMPLATES (adapt to actual schema):
        1. Basic Counts with Existence Check:
          WITH base_counts AS (
            SELECT
              t1.id,  -- Replace with actual primary key
              CASE WHEN EXISTS (
                SELECT 1 FROM related_table t2
                WHERE t2.foreign_key = t1.id  -- Replace with actual relationship
              ) THEN 1 ELSE 0 END as has_related
            FROM main_table t1  -- Replace with actual table
          )
          SELECT
            COUNT(*) as total_count,
            SUM(has_related) as related_count
          FROM base_counts;

        2. Segmentation with Totals:
          WITH base_data AS (
            SELECT
              t1.id,
              CAST(t1.value_column AS NUMERIC) as value,  -- Always CAST numeric values
              CAST(COALESCE(t2.related_value, 0) AS NUMERIC) as related_value
            FROM main_table t1
            LEFT JOIN related_table t2 
              ON t2.foreign_key = t1.id  -- Replace with actual relationship
          ),
          metrics AS (
            SELECT
              'Segment' as result_type,
              CASE WHEN condition THEN 'A' ELSE 'B' END as segment,
              COUNT(*) as count,
              AVG(value) as avg_value,
              SUM(value) as total_value,
              ROUND(
                AVG(CAST(related_value AS NUMERIC) * 100.0 / 
                    NULLIF(CAST(value AS NUMERIC), 0)
                ), 2) as percentage
            FROM base_data
            GROUP BY CASE WHEN condition THEN 'A' ELSE 'B' END
          ),
          totals AS (
            SELECT
              'Total' as result_type,
              'Total' as segment,
              COUNT(*) as count,
              AVG(value) as avg_value,
              SUM(value) as total_value,
              ROUND(
                AVG(CAST(related_value AS NUMERIC) * 100.0 / 
                    NULLIF(CAST(value AS NUMERIC), 0)
                ), 2) as percentage
            FROM base_data
          )
          SELECT * FROM metrics
          UNION ALL
          SELECT * FROM totals
          ORDER BY result_type DESC, segment;

        3. Correlation Analysis:
          WITH base_data AS (
            SELECT
              t1.id,
              CAST(t1.value_column AS NUMERIC) as x,
              CAST(COALESCE(t2.related_value, 0) AS NUMERIC) as y
            FROM main_table t1
            LEFT JOIN related_table t2 ON t2.foreign_key = t1.id
          ),
          segment_data AS (
            SELECT
              segment,
              AVG(x) as avg_x,
              AVG(y) as avg_y,
              COUNT(*) as n,
              STDDEV_POP(x) as stddev_x,
              STDDEV_POP(y) as stddev_y,
              SUM(x * y) as sum_xy,
              SUM(x) as sum_x,
              SUM(y) as sum_y
            FROM (
              SELECT
                CASE WHEN condition THEN 'A' ELSE 'B' END as segment,
                x, y
              FROM base_data
            ) t
            GROUP BY segment
          )
          SELECT
            segment,
            ROUND(
              (n * sum_xy - sum_x * sum_y) / 
              NULLIF(
                (SQRT(NULLIF(n * SUM(x * x) - SUM(x) * SUM(x), 0)) * 
                 SQRT(NULLIF(n * SUM(y * y) - SUM(y) * SUM(y), 0))),
                0
              ),
              4
            ) as correlation
          FROM segment_data
          GROUP BY segment;

        4. Percentile-based Segmentation:
          WITH base_data AS (
            SELECT 
              t1.*,
              CAST(t1.value_column AS NUMERIC) as numeric_value,
              NTILE(10) OVER (ORDER BY CAST(t1.value_column AS NUMERIC)) as segment
            FROM main_table t1
          ),
          segment_metrics AS (
            SELECT 
              segment,
              COUNT(*) as count,
              AVG(numeric_value) as avg_value,
              MIN(numeric_value) as min_value,
              MAX(numeric_value) as max_value
            FROM base_data
            GROUP BY segment
          )
          SELECT * FROM segment_metrics
          ORDER BY segment;`;

  const ai = new Anthropic({ apiKey });
  const completion = await ai.messages.create({
    model: "claude-3-5-sonnet-20241022",
    max_tokens: 8192,
    messages: [
      {
        role: "user",
        content: systemPrompt + "\n\n" + question
      }
    ],
    temperature: 0
  });

  const sqlQuery = completion.content[0]?.type === 'text' ? completion.content[0].text : null;
  if (!sqlQuery) {
    throw new Error('Failed to generate SQL query: No response from Anthropic');
  }

  if (sqlQuery.startsWith('ERROR:')) {
    throw new Error(sqlQuery);
  }

  return sqlQuery;
}

function formatDataResponse(rows: any[], instructions?: string): string {
  let response = '';

  // Handle empty results
  if (!rows || rows.length === 0) {
    response = "No data found for your query.";
  } else {
    try {
      const sanitizedRows = rows.map(row => {
        const sanitizedRow: any = {};
        for (const [key, value] of Object.entries(row)) {
          sanitizedRow[key] = typeof value === 'bigint' || typeof value === 'number' 
            ? value.toString() 
            : value;
        }
        return sanitizedRow;
      });

      response = JSON.stringify(sanitizedRows, null, 2);
    } catch (error) {
      throw new Error(`Error formatting database response: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  // Add instructions if provided
  if (instructions) {
    response = `Instructions for the following content: ${instructions}\n\n${response}`;
  }

  return response;
}

function formatQueryResponse(sqlQuery: string): string {
  return sqlQuery;
}

/*
 * CRITICAL NOTE: All patterns must be schema-agnostic and generalizable!
 * The examples use generic table/column names that must be replaced
 * with actual schema elements from the provided schema information.
 * 
 * ERROR HISTORY AND SOLUTIONS LOG
 * ------------------------------
 * This section tracks common errors and their solutions to ensure
 * we maintain compatibility across all use cases.
 * 
 * 1. "missing FROM-clause entry for table"
 *    Problem: Incorrect table references or missing schema validation
 *    Solution: 
 *    - Verify all tables exist in schema before using
 *    - Use proper table aliases consistently
 *    - Ensure JOINs match actual schema relationships
 *    - Check foreign key constraints in schema
 * 
 * 2. "syntax error at or near 'I'"
 *    Problem: Model including explanatory text in SQL output
 *    Solution: 
 *    - Strict response format rules
 *    - NO explanations or text, only SQL
 * 
 * 3. "invalid UNION/INTERSECT/EXCEPT ORDER BY clause"
 *    Problem: Incorrect ORDER BY with UNION
 *    Solution: 
 *    - Use result_type column for sorting
 *    - ORDER BY only at final query level
 *    - Consistent column names across UNION
 * 
 * 4. "aggregate functions are not allowed in GROUP BY"
 *    Problem: Calculated fields in GROUP BY
 *    Solution: 
 *    - Pre-calculate values in earlier CTEs
 *    - Group only by raw columns or simple CASE statements
 *    - No aggregates in GROUP BY clause
 * 
 * 5. "aggregate function calls cannot contain window function calls"
 *    Problem: Mixing window functions with aggregates
 *    Solution: 
 *    - Calculate window functions in separate CTE
 *    - Use results in subsequent aggregations
 *    - Keep window functions and aggregates separate
 * 
 * 6. "window functions are not allowed in GROUP BY"
 *    Problem: Window functions in GROUP BY clause
 *    Solution: 
 *    - Calculate window functions first in CTE
 *    - Group by resulting segment/value
 *    - Never use window function results directly in GROUP BY
 * 
 * IMPLEMENTATION REQUIREMENTS:
 * 1. Schema Awareness
 *    - All queries must be built using actual schema information
 *    - Verify table and column existence before use
 *    - Use proper relationships from schema constraints
 * 
 * 2. Safe Calculations
 *    - Always CAST numeric values explicitly
 *    - Use NULLIF for division operations
 *    - Handle NULL values appropriately
 * 
 * 3. Query Structure
 *    - Use CTEs for complex calculations
 *    - Separate window functions from aggregations
 *    - Proper table aliases and column qualification
 * 
 * 4. Performance
 *    - Limit results appropriately
 *    - Use indexes when available (usually primary keys)
 *    - Filter early in the query chain
 */