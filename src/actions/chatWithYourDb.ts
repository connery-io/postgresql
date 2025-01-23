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

    Return ONLY the raw SQL query without any formatting, markdown, or code blocks.

    CRITICAL RULES TO PREVENT COMMON ERRORS:

    1. Table References:
      - Verify all tables exist in schema before using them
      - Use consistent table aliases throughout the query
      - Only JOIN tables with valid relationships from schema constraints
      - Always qualify column names with table aliases

    2. Query Response Format:
      - Return ONLY the SQL query
      - No explanatory text or markdown
      - No code blocks or formatting

    3. UNION/Compound Queries:
      - Place ORDER BY only at the final query level
      - Ensure consistent column names and types across UNION parts
      - Add descriptive labels for different result sets
      - Use CTEs for complex UNION operations

    4. Aggregation Rules:
      - Never use aggregates in GROUP BY clauses
      - Group only by raw columns or simple CASE expressions
      - Pre-calculate complex values in CTEs before grouping

    5. Window Function Usage:
      - Never mix window functions with aggregates in the same expression
      - Calculate window functions in separate CTEs first
      - Use results in subsequent operations

    6. Window Functions in Groups:
      - Never use window functions in GROUP BY clauses
      - Calculate window functions in separate CTEs
      - Group by resulting values only

    7. Multi-level Aggregations:
      - Always use hierarchical aggregation for nested data:
        * First CTE: Aggregate details to parent level
        * Second CTE: Segment parents based on characteristics
        * Final CTE: Calculate overall totals if needed
      - For entity-level averages:
        * WRONG: AVG(detail.value)
        * RIGHT: AVG(parent_totals.total_value)
      - For entity grouping:
        * WRONG: GROUP BY detail.attribute > 0
        * RIGHT: GROUP BY parent_level.has_attribute
      - For MECE (Mutually Exclusive, Collectively Exhaustive) results:
        * WRONG: COUNT(DISTINCT parent_id) directly from details
        * RIGHT: COUNT(*) from parent-level CTE
      Example pattern:
        WITH detail_totals AS (
          -- First aggregate all details to parent level
          SELECT 
            parent_id,
            SUM(quantity) as total_quantity,
            SUM(amount) as total_amount,
            MAX(CASE WHEN attribute > 0 THEN 1 ELSE 0 END) as has_attribute,
            SUM(amount * attribute) as attribute_amount
          FROM detail_table
          GROUP BY parent_id
        ),
        parent_segments AS (
          -- Then segment based on parent-level characteristics
          SELECT
            CASE WHEN has_attribute = 1 THEN 'With Attribute' 
                 ELSE 'Without Attribute' END as segment,
            COUNT(*) as total_parents,
            ROUND(CAST(AVG(total_quantity) AS NUMERIC), 2) as avg_quantity,
            ROUND(CAST(AVG(total_amount) AS NUMERIC), 2) as avg_amount,
            ROUND(CAST(AVG(attribute_amount) AS NUMERIC), 2) as avg_attr_amount,
            ROUND(CAST(SUM(attribute_amount) * 100.0 / NULLIF(SUM(total_amount), 0) AS NUMERIC), 2) as attr_percentage
          FROM detail_totals
          GROUP BY has_attribute
        )

    8. Query Optimization:
      - Keep queries as simple as possible while meeting requirements
      - Avoid unnecessary JOINs
      - Filter early in CTEs
      - Use indexes (typically primary keys) when available

    9. Numeric Calculations:
      - Always CAST numeric inputs before ROUND: ROUND(CAST(value AS NUMERIC), 2)
      - For percentages: ROUND(CAST(value * 100.0 AS NUMERIC), 2)
      - For monetary values: ROUND(CAST(value AS NUMERIC), 2)
      - For ratios/divisions: ROUND(CAST(CAST(numerator AS NUMERIC) / NULLIF(denominator, 0) AS NUMERIC), 2)
      - Handle NULLs: COALESCE(value, 0)

    IMPLEMENTATION REQUIREMENTS:
    - Generate only SELECT queries (no modifications)
    - Include LIMIT ${maxRows} in final results
    - Use explicit column names (no SELECT *)
    - Add ORDER BY when relevant to the question
    - Include inline comments with -- to explain logic
    - For calculations:
      * Use CAST(value AS NUMERIC) explicitly
      * Apply ROUND(value, 2) for decimals
      * Use COALESCE/NULLIF for NULL handling
    - For complex queries:
      * Use WITH clauses (CTEs) for readability
      * Break down complex logic into steps
      * Add descriptive CTE names

    If you cannot construct a valid query using only the available schema, respond with an error message starting with "ERROR:".`;

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
 * 7. "Non-MECE Results in Multi-Level Aggregations"
 *    Problem: Incorrect aggregation levels leading to wrong averages and counts
 *    Solution: 
 *    - Always use three-step aggregation for hierarchical data:
 *      1. Aggregate details to parent level (all metrics per parent)
 *      2. Segment parents based on characteristics
 *      3. Calculate overall totals if needed
 *    - Common mistakes and fixes:
 *      Instead of:
 *        SELECT 
 *          has_attribute,
 *          COUNT(DISTINCT parent_id) as total,
 *          AVG(amount) as avg_amount
 *        FROM details
 *        GROUP BY has_attribute
 *      Use:
 *        WITH parent_totals AS (
 *          SELECT 
 *            parent_id,
 *            MAX(CASE WHEN attribute > 0 THEN 1 ELSE 0 END) as has_attribute,
 *            SUM(amount) as total_amount
 *          FROM details
 *          GROUP BY parent_id
 *        )
 *        SELECT
 *          has_attribute,
 *          COUNT(*) as total,
 *          AVG(total_amount) as avg_amount
 *        FROM parent_totals
 *        GROUP BY has_attribute
 *    Testing:
 *    - Compare results with manual calculations for a small dataset
 *    - Verify parent counts match between segments and totals
 *    - Check that averages are calculated at the correct level
 *    - Test with parents having varying numbers of detail records
 *    - Test with mixed attribute values within the same parent
 * 
 * 8. "Overloaded Error"
 *    Problem: Query too complex or taking too long
 *    Solution: 
 *    - Use simpler queries for basic aggregations
 *    - Add timeout handling
 *    - Include simple aggregation pattern
 *    - Avoid unnecessary JOINs for simple calculations
 * 
 * 9. "function round(double precision, integer) does not exist"
 *    Problem: PostgreSQL type mismatch with ROUND function
 *    Solution: 
 *    - Always CAST to NUMERIC before ROUND
 *    - Use proper numeric calculation patterns:
 *      * Percentages: ROUND(CAST(value * 100.0 AS NUMERIC), 2)
 *      * Money: ROUND(CAST(value AS NUMERIC), 2)
 *      * Ratios: ROUND(CAST(CAST(num AS NUMERIC) / NULLIF(denom, 0) AS NUMERIC), 2)
 *    Example fix:
 *      Instead of:
 *        ROUND(price * quantity, 2)
 *      Use:
 *        ROUND(CAST(price * quantity AS NUMERIC), 2)
 *    Testing:
 *      - Test with decimal values
 *      - Test with integer values
 *      - Test with NULL values
 *      - Test with zero denominators in divisions
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