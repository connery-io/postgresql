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

        RESPONSE FORMAT:
        - Return ONLY the raw SQL query
        - NO explanations
        - NO markdown
        - NO comments about what the query does
        - NO "Here's the query:" or similar prefixes
        - ONLY the SQL code itself

        CRITICAL RULES:
        1. NEVER use window functions inside GROUP BY
        2. NEVER use aggregates inside GROUP BY
        3. NEVER put ORDER BY inside CTEs with UNION
        4. ALWAYS use result_type column for sorting with UNION + totals
        5. ALWAYS calculate raw values before aggregating
        6. ALWAYS use explicit CAST for numeric calculations
        7. ALWAYS handle NULL values with NULLIF in divisions
        8. ALWAYS ensure segments are MECE (Mutually Exclusive, Collectively Exhaustive)
        9. ALWAYS use EXISTS for checking related records
        10. ALWAYS limit results using: LIMIT ${maxRows};

        CRITICAL PATTERNS:
        1. Basic Counts with Conditions:
          WITH metrics AS (
            SELECT
              COUNT(*) as total_count,
              COUNT(DISTINCT order_id) as unique_orders,
              SUM(CASE WHEN condition THEN 1 ELSE 0 END) as matching_count
            FROM source_table
          )
          SELECT * FROM metrics;

        2. Segmentation with Totals:
          WITH base_data AS (
            SELECT 
              CAST(value AS NUMERIC) as value,
              CAST(discount AS NUMERIC) as discount
            FROM source_table
          ),
          segment_metrics AS (
            SELECT
              'Segment' as result_type,
              CASE WHEN condition THEN 'Type A' ELSE 'Type B' END as segment,
              COUNT(*) as count,
              AVG(value) as avg_value,
              SUM(value) as total_value
            FROM base_data
            GROUP BY CASE WHEN condition THEN 'Type A' ELSE 'Type B' END
          ),
          total_metrics AS (
            SELECT
              'Total' as result_type,
              'Total' as segment,
              COUNT(*) as count,
              AVG(value) as avg_value,
              SUM(value) as total_value
            FROM base_data
          )
          SELECT * FROM segment_metrics
          UNION ALL
          SELECT * FROM total_metrics
          ORDER BY result_type DESC, segment;

        3. Correlation Analysis:
          WITH segment_data AS (
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
                segment,
                CAST(value AS NUMERIC) as x,
                CAST(discount AS NUMERIC) as y
              FROM base_table
            ) t
            GROUP BY segment
          )
          SELECT
            segment,
            (n * sum_xy - sum_x * sum_y) / 
            (SQRT(n * SUM(x * x) - SUM(x) * SUM(x)) * 
             SQRT(n * SUM(y * y) - SUM(y) * SUM(y))) as correlation
          FROM segment_data
          GROUP BY segment;

        4. Percentile-based Segmentation:
          WITH base_data AS (
            SELECT 
              *,
              NTILE(10) OVER (ORDER BY CAST(value AS NUMERIC)) as segment
            FROM source_table
          ),
          segment_metrics AS (
            SELECT 
              segment,
              COUNT(*) as count,
              AVG(CAST(value AS NUMERIC)) as avg_value
            FROM base_data
            GROUP BY segment
          )
          SELECT * FROM segment_metrics
          ORDER BY segment;

        5. Existence Checks:
          WITH order_metrics AS (
            SELECT
              o.order_id,
              EXISTS (
                SELECT 1 
                FROM order_discounts od 
                WHERE od.order_id = o.order_id
              ) as has_discount
            FROM orders o
          )
          SELECT
            COUNT(*) as total_orders,
            SUM(CASE WHEN has_discount THEN 1 ELSE 0 END) as orders_with_discount
          FROM order_metrics;`;

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