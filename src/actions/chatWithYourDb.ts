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
        
        Important: Return ONLY the raw SQL query without any formatting, markdown, or code blocks.
        
        Rules:
        - Use ONLY tables and columns that exist in the provided schema information
        - Do not make assumptions about columns that aren't explicitly listed in the schema
        - Generate only SELECT queries (no INSERT, UPDATE, DELETE, etc.)
        - Ensure queries are optimized for performance:
          * Limit complex calculations to necessary rows
          * Use WHERE clauses before aggregations
          * Keep CTEs simple and focused
          * Avoid multiple passes over large datasets
          * Use indexes when available (usually primary keys)
        - Include relevant JOINs when needed
        - Add inline comments with -- to explain the query
        - Limit results to ${maxRows} rows using LIMIT clause
        - Use explicit column names instead of SELECT *
        - Add ORDER BY clauses when relevant
        - When using numeric calculations:
          * Cast numeric values explicitly (e.g., CAST(value AS NUMERIC))
          * Use ROUND(CAST(value AS NUMERIC), 2) for decimal places
          * Handle NULL values with COALESCE
          * For averages, use AVG(CAST(column AS NUMERIC))
          * For sums, use SUM(CAST(column AS NUMERIC))
          * For counts, use COUNT(*) when possible
        - For aggregations and grouping:
          * Calculate base values before aggregating using CTEs:
            WITH base_calculations AS (
              SELECT 
                order_id,
                CAST(value AS NUMERIC) as value,
                CAST(discount AS NUMERIC) as discount
              FROM orders
            ),
            segment_metrics AS (
              SELECT
                CASE WHEN discount > 0 THEN 'With Discount' 
                     ELSE 'Without Discount' 
                END as segment,
                COUNT(*) as order_count,
                AVG(value) as avg_value,
                SUM(value) as total_value,
                AVG(discount) as avg_discount,
                SUM(discount) as total_discount
              FROM base_calculations
              GROUP BY 
                CASE WHEN discount > 0 THEN 'With Discount' 
                     ELSE 'Without Discount' 
                END
            )
            SELECT * FROM segment_metrics;
          * Never use calculated fields or aggregates in GROUP BY
          * Pre-calculate complex values in earlier CTEs
          * Use simple CASE statements for grouping
          * Structure multi-level aggregations:
            1. Base calculations (raw values, type casting)
            2. Record-level calculations (per order/item)
            3. Group-level aggregations (averages, totals)
          * For percentage calculations:
            - Calculate components separately
            - Use NULLIF for division to avoid divide by zero
            - Example:
              ROUND(
                CAST(discount_value AS NUMERIC) * 100.0 / 
                NULLIF(CAST(total_value AS NUMERIC), 0),
                2
              ) as discount_percentage
        - For statistical analysis and outliers:
          * Use CTEs to calculate statistics separately
          * Calculate quartiles using percentile_cont without OVER clause
          * For outliers, use 1.5 * IQR method with pre-calculated quartiles
          * Avoid window functions with ordered-set aggregates
        - For date/time calculations:
          * Always cast date/time fields before operations
          * Use date_part('field', CAST(column AS timestamp))
          * Use date_trunc('field', CAST(column AS timestamp))
          * For intervals, use CAST(value AS interval)
          * Avoid direct numeric operations on dates
        - For customer behavior analysis:
          * Pre-calculate aggregates in CTEs
          * Ensure proper type casting for all date/time fields
          * Use count(*) instead of count(column) when possible
          * Always cast numeric aggregations to NUMERIC
          * For segmentation, use CASE statements with explicit casts
        - For table references and aliases:
          * Always qualify column names with table aliases
          * Define each CTE with a clear purpose
          * Reference the correct CTE in subsequent calculations
          * Use meaningful alias names (e.g., orders o, customers c)
          * Ensure all referenced tables exist in FROM clause
        - For CTEs and subqueries:
          * Always name CTEs descriptively (e.g., avg_discounts, order_totals)
          * Reference CTEs in the main query using their full names
          * Include all necessary CTEs in the WITH clause
          * Chain CTEs in logical order
          * Ensure each CTE is properly referenced
        - Query optimization requirements:
          * Limit to essential joins only
          * Filter data early in the query
          * Use subqueries sparingly
          * Avoid cross joins
          * Keep window functions minimal
        - Do not include markdown code blocks or SQL syntax highlighting in your response
        - Do not include any other text in your response
        - If you cannot construct a query using only the available columns, respond with an error message starting with "ERROR:"
        - For segmentation and grouping logic:
          * Define mutually exclusive conditions
          * Use EXISTS/NOT EXISTS for related table checks
          * Avoid counting same records multiple times
          * For "any" conditions, use EXISTS subqueries
          * For "all" conditions, use NOT EXISTS with negation
          * Use CASE WHEN for clear segment definitions
          * Verify segments are complete and non-overlapping
          * Document segment logic in comments
        - For aggregations across related tables:
          * Use EXISTS for "at least one" relationships
          * Use NOT EXISTS for "none" relationships
          * Avoid JOIN when checking existence is sufficient
          * Count distinct primary keys to prevent duplicates
          * Verify totals match expected row counts
        - For hierarchical data analysis:
          * When analyzing parent records (e.g., orders, invoices):
            - Consider all child records (e.g., line items, details) for segmentation
            - Use EXISTS/NOT EXISTS to check conditions across child records
            - For "records with condition":
              EXISTS (SELECT 1 FROM child_table WHERE parent_id = parent.id AND condition)
            - For "records without condition":
              NOT EXISTS (SELECT 1 FROM child_table WHERE parent_id = parent.id AND condition)
          * Calculate aggregates at the appropriate level
          * Document the analysis level in comments
          * Verify parent-child relationships using schema constraints
        - For segmentation analysis:
          * Always ensure segments are MECE (Mutually Exclusive, Collectively Exhaustive)
          * For combining segments with totals, use this pattern:
            WITH segment_data AS (
              SELECT 
                CASE WHEN condition THEN 'Segment A' ELSE 'Segment B' END as segment,
                metrics...
              FROM source_table
              GROUP BY CASE WHEN condition THEN 'Segment A' ELSE 'Segment B' END
            ),
            total_data AS (
              SELECT 
                'Total' as segment,
                metrics...
              FROM source_table
            )
            SELECT * FROM segment_data
            UNION ALL
            SELECT * FROM total_data
            ORDER BY 
              CASE 
                WHEN segment = 'Total' THEN 2
                ELSE 1
              END,
              segment;
          * For segment comparisons:
            - Calculate all metrics within each CTE
            - Use clear segment names
            - Ensure consistent column types across UNION
            - Place ORDER BY only in the final query
            - Example structure for comparison analysis:
              WITH metrics_by_segment AS (
                SELECT
                  CASE 
                    WHEN condition THEN 'With Condition'
                    ELSE 'Without Condition'
                  END as segment,
                  COUNT(*) as count,
                  AVG(CAST(value AS NUMERIC)) as avg_value,
                  SUM(CAST(value AS NUMERIC)) as total_value
                FROM source_table
                GROUP BY 
                  CASE 
                    WHEN condition THEN 'With Condition'
                    ELSE 'Without Condition'
                  END
              ),
              total_metrics AS (
                SELECT
                  'Total' as segment,
                  COUNT(*) as count,
                  AVG(CAST(value AS NUMERIC)) as avg_value,
                  SUM(CAST(value AS NUMERIC)) as total_value
                FROM source_table
              )
              SELECT * FROM metrics_by_segment
              UNION ALL
              SELECT * FROM total_metrics
              ORDER BY 
                CASE 
                  WHEN segment = 'Total' THEN 2 
                  ELSE 1 
                END,
                segment;
          * Add validation comments showing segment math
          * Ensure segment values sum up to totals
        - For segment-level correlations:
          * Calculate segments in steps:
            WITH base_data AS (
              SELECT *,
                NTILE(N) OVER (ORDER BY value) as segment
              FROM source_table
            ),
            metrics AS (
              SELECT 
                segment,
                COUNT(*) as count,
                AVG(value1) as avg1,
                AVG(value2) as avg2
              FROM base_data
              GROUP BY segment
            )
            SELECT * FROM metrics
            ORDER BY segment;
          * Never use window functions in GROUP BY or aggregates
          * Calculate NTILE() before any aggregations
          * Use simple GROUP BY on pre-calculated segments`;

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