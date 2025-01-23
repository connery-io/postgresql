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
      - Place ORDER BY only at the final query level, never inside CTEs or UNION parts
      - Ensure consistent column names and types across all UNION parts
      - For segmentation with totals:
        * WRONG: Using ORDER BY inside WITH clause or UNION parts
        * RIGHT: Single ORDER BY after all UNIONs
      - For sorting segments with totals:
        * Use CASE statement in final ORDER BY
        * Put totals first/last using segment name
      Example pattern:
        WITH base_segments AS (
          SELECT segment, metrics...
          FROM parent_totals
          GROUP BY segment
          
          UNION ALL
          
          SELECT 'Total' as segment, metrics...
          FROM parent_totals
        )
        SELECT *
        FROM base_segments
        ORDER BY
          CASE 
            WHEN segment = 'Total' THEN 2
            WHEN segment = 'First Segment' THEN 0
            ELSE 1
          END;

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
        * WRONG: AVG(detail.value) directly from details
        * RIGHT: AVG(parent_totals.total_value) from parent-level CTE
      - For segment averages:
        * WRONG: AVG(value) - can be skewed by outliers
        * RIGHT: SUM(value) / COUNT(*) - maintains arithmetic mean
      - For total averages:
        * WRONG: AVG of segment averages
        * RIGHT: SUM of all values / COUNT of all parents
      - For consistent segmentation:
        * Step 1: Determine parent-level attributes
          WITH parent_attrs AS (
            SELECT parent_id, 
                   MAX(CASE WHEN attribute > 0 THEN 1 ELSE 0 END) as has_attr
            FROM details
            GROUP BY parent_id
          )
        * Step 2: Calculate parent-level totals
          parent_totals AS (
            SELECT p.parent_id, 
                   p.has_attr,
                   SUM(d.amount) as total_amount
            FROM parent_attrs p
            JOIN details d ON d.parent_id = p.parent_id
            GROUP BY p.parent_id, p.has_attr
          )
        * Step 3: Create segments
          segments AS (
            SELECT
              CASE WHEN has_attr = 1 THEN 'With' ELSE 'Without' END as segment,
              COUNT(*) as parent_count,
              SUM(total_amount) as segment_total,
              SUM(total_amount) / COUNT(*) as segment_average
            FROM parent_totals
            GROUP BY has_attr
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

    10. Statistical Calculations:
       - For complex statistics requiring multiple aggregations:
         * WRONG: Mixing window and aggregate functions directly
         * RIGHT: Use staged CTEs to build up calculations
       - For correlations and statistical measures:
         * Step 1: Calculate base metrics per entity with unique names
         * Step 2: Calculate statistical components with qualified references
         * Step 3: Combine into final formula
       - For grouped statistics:
         * WRONG: Using non-aggregated columns from stats
         * RIGHT: Include all grouping columns in each CTE
       Example pattern for grouped correlation:
         WITH base_metrics AS (
           -- First get metrics per entity with unique names
           SELECT 
             parent_id,
             segment_id,  -- Include grouping columns
             SUM(amount) / NULLIF(COUNT(*), 0) as entity_avg_amount,
             SUM(attribute) / NULLIF(COUNT(*), 0) as entity_avg_attribute
           FROM details
           GROUP BY parent_id, segment_id
         ),
         stats_per_segment AS (
           -- Calculate components per segment
           SELECT
             segment_id,  -- Include grouping columns
             COUNT(*) as segment_n,
             AVG(entity_avg_amount) as segment_avg_x,
             AVG(entity_avg_attribute) as segment_avg_y,
             STDDEV_POP(entity_avg_amount) as segment_stddev_x,
             STDDEV_POP(entity_avg_attribute) as segment_stddev_y,
             SUM(entity_avg_amount * entity_avg_attribute) as segment_sum_xy,
             SUM(entity_avg_amount) as segment_sum_x,
             SUM(entity_avg_attribute) as segment_sum_y,
             SUM(POWER(entity_avg_amount, 2)) as segment_sum_x2,
             SUM(POWER(entity_avg_attribute, 2)) as segment_sum_y2
           FROM base_metrics
           GROUP BY segment_id
         )
         -- Calculate correlation per segment
         SELECT 
           segment_id,
           (segment_n * segment_sum_xy - segment_sum_x * segment_sum_y) /
           NULLIF(
             (SQRT(segment_n * segment_sum_x2 - POWER(segment_sum_x, 2)) *
              SQRT(segment_n * segment_sum_y2 - POWER(segment_sum_y, 2))),
             0
           ) as segment_correlation
         FROM stats_per_segment;

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
 *    Problem: Attempting to use ORDER BY within UNION parts
 *    Solution: 
 *    - Move all ORDER BY clauses outside the UNION
 *    - Use CASE statement in final ORDER BY for segment ordering
 *    Example fix:
 *      Instead of:
 *        WITH segments AS (
 *          SELECT ... FROM parent_totals
 *          GROUP BY segment
 *          ORDER BY segment  -- Wrong: ORDER BY inside CTE
 *          
 *          UNION ALL
 *          
 *          SELECT ... FROM parent_totals
 *          ORDER BY segment  -- Wrong: ORDER BY in UNION part
 *        )
 *      Use:
 *        WITH segments AS (
 *          SELECT ... FROM parent_totals
 *          GROUP BY segment
 *          
 *          UNION ALL
 *          
 *          SELECT ... FROM parent_totals
 *        )
 *        SELECT *
 *        FROM segments
 *        ORDER BY  -- Correct: Single ORDER BY after UNION
 *          CASE 
 *            WHEN segment = 'Total' THEN 2
 *            WHEN segment = 'First' THEN 0
 *            ELSE 1
 *          END;
 *    Testing:
 *    - Verify segment order is correct
 *    - Check that totals appear in desired position
 *    - Test with different segment names
 *    - Ensure all UNION parts have matching column counts and types
 * 
 * 4. "aggregate functions are not allowed in GROUP BY"
 *    Problem: Calculated fields in GROUP BY
 *    Solution: 
 *    - Pre-calculate values in earlier CTEs
 *    - Group only by raw columns or simple CASE statements
 *    - No aggregates in GROUP BY clause
 * 
 * 5. "aggregate function calls cannot contain window function calls"
 *    Problem: Attempting to combine window functions with aggregates
 *    Solution: 
 *    - Break calculation into separate CTEs
 *    - Calculate window functions first
 *    - Use results in subsequent aggregations
 *    Example fix:
 *      Instead of:
 *        SELECT CORR(AVG(amount) OVER (PARTITION BY parent_id), 
 *                   AVG(attribute) OVER (PARTITION BY parent_id))
 *        FROM details
 *      Use:
 *        WITH per_parent AS (
 *          SELECT parent_id,
 *                 AVG(amount) as avg_amount,
 *                 AVG(attribute) as avg_attribute
 *          FROM details
 *          GROUP BY parent_id
 *        )
 *        SELECT CORR(avg_amount, avg_attribute)
 *        FROM per_parent
 *    Testing:
 *    - Verify results match manual calculations
 *    - Test with different window sizes
 *    - Check handling of NULL values
 *    - Test with single-row groups
 *    - Validate statistical significance
 * 
 * 6. "window functions are not allowed in GROUP BY"
 *    Problem: Window functions in GROUP BY clause
 *    Solution: 
 *    - Calculate window functions first in CTE
 *    - Group by resulting segment/value
 *    - Never use window function results directly in GROUP BY
 * 
 * 7. "Non-MECE Results in Multi-Level Aggregations"
 *    Problem: Incorrect aggregation levels leading to wrong segment totals
 *    Solution: 
 *    - Never segment at detail level when counting or averaging parent entities
 *    - Always follow this pattern:
 *      1. Aggregate ALL metrics to parent level first
 *      2. Then segment based on parent characteristics
 *      3. Finally add totals if needed
 *    Common mistakes and fixes:
 *    Instead of:
 *      SELECT 
 *        CASE WHEN d.attribute > 0 THEN 'With' ELSE 'Without' END as segment,
 *        COUNT(DISTINCT d.parent_id) as total,
 *        AVG(d.amount) as avg_amount
 *      FROM details d
 *      GROUP BY CASE WHEN d.attribute > 0 THEN 'With' ELSE 'Without' END
 *    Use:
 *      WITH parent_totals AS (
 *        SELECT 
 *          parent_id,
 *          SUM(amount) as total_amount,
 *          MAX(CASE WHEN attribute > 0 THEN 1 ELSE 0 END) as has_attribute
 *        FROM details
 *        GROUP BY parent_id
 *      )
 *      SELECT
 *        CASE WHEN has_attribute = 1 THEN 'With' ELSE 'Without' END as segment,
 *        COUNT(*) as total,
 *        AVG(total_amount) as avg_amount
 *      FROM parent_totals
 *      GROUP BY has_attribute
 *    Testing:
 *    - Compare segment counts: sum should equal total parents
 *    - Check parent appears in only one segment
 *    - Verify averages match manual calculations
 *    - Test with parent having mixed attribute values
 *    - Test with uneven distribution of details per parent
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
 * 12. "Incorrect Segment Averages"
 *     Problem: Segment averages don't roll up to total average
 *     Solution: 
 *     - Use SUM/COUNT instead of AVG for consistent arithmetic means
 *     - Calculate totals from base data, not segment averages
 *     - Verify calculations with these tests:
 *       * SUM(segment_count × segment_average) should equal total_sum
 *       * Overall average = total_sum / total_count
 *     Example fix:
 *       Instead of:
 *         SELECT 
 *           segment,
 *           AVG(value) as avg_value  -- Wrong: can be skewed
 *         FROM segments
 *         GROUP BY segment
 *       Use:
 *         SELECT
 *           segment,
 *           SUM(value) / COUNT(*) as avg_value  -- Right: arithmetic mean
 *         FROM segments
 *         GROUP BY segment
 *     Testing:
 *     - Calculate weighted average of segments (count × average)
 *     - Compare to total sum / total count
 *     - Test with skewed value distributions
 *     - Verify segment counts sum to total
 *     - Check for proper handling of outliers
 * 
 * 13. "Inconsistent Segmentation Results"
 *     Problem: Same query produces different segment results
 *     Solution: 
 *     - Use strict three-step segmentation pattern:
 *       1. Determine parent-level attributes first (separate CTE)
 *       2. Calculate parent-level totals using these attributes
 *       3. Segment using the parent-level attributes
 *     - Never mix segmentation and aggregation in same step
 *     Example fix:
 *       Instead of:
 *         SELECT 
 *           CASE WHEN d.attribute > 0 THEN 'With' ELSE 'Without' END,
 *           COUNT(DISTINCT d.parent_id),
 *           SUM(d.amount)
 *         FROM details d
 *         GROUP BY CASE WHEN d.attribute > 0 THEN 'With' ELSE 'Without' END
 *       Use:
 *         WITH parent_attrs AS (
 *           SELECT parent_id, MAX(attribute > 0) as has_attr
 *           FROM details
 *           GROUP BY parent_id
 *         ),
 *         parent_totals AS (
 *           SELECT p.parent_id, p.has_attr, SUM(d.amount) as total
 *           FROM parent_attrs p
 *           JOIN details d ON d.parent_id = p.parent_id
 *           GROUP BY p.parent_id, p.has_attr
 *         )
 *         SELECT 
 *           CASE WHEN has_attr THEN 'With' ELSE 'Without' END,
 *           COUNT(*),
 *           SUM(total)
 *         FROM parent_totals
 *         GROUP BY has_attr
 *     Testing:
 *     - Run query multiple times to verify consistent results
 *     - Compare segment counts with direct parent counts
 *     - Verify no parent appears in multiple segments
 *     - Test with parents having mixed attribute values
 *     - Check that parent-level metrics match when calculated different ways
 * 
 * 14. "Statistical Calculations:
 *     - For complex statistics requiring multiple aggregations:
 *       * WRONG: Mixing window and aggregate functions directly
 *       * RIGHT: Use staged CTEs to build up calculations
 *     - For correlations and statistical measures:
 *       * Step 1: Calculate base metrics per entity with unique names
 *       * Step 2: Calculate statistical components with qualified references
 *       * Step 3: Combine into final formula
 *     - For column references:
 *       * WRONG: Using ambiguous column names across CTEs
 *       * RIGHT: Using unique or qualified column names
 *       * RIGHT: Using table aliases for clarity
 *     Example pattern for correlation:
 *       WITH base_metrics AS (
 *         -- First get metrics per entity with unique names
 *         SELECT 
 *           parent_id,
 *           SUM(amount) / NULLIF(COUNT(*), 0) as entity_avg_amount,
 *           SUM(attribute) / NULLIF(COUNT(*), 0) as entity_avg_attribute
 *         FROM details
 *         GROUP BY parent_id
 *       ),
 *       stats AS (
 *         -- Then calculate statistical components with unique names
 *         SELECT
 *           COUNT(*) as n,
 *           AVG(entity_avg_amount) as population_avg_x,
 *           AVG(entity_avg_attribute) as population_avg_y,
 *           STDDEV_POP(entity_avg_amount) as population_stddev_x,
 *           STDDEV_POP(entity_avg_attribute) as population_stddev_y,
 *           SUM(entity_avg_amount * entity_avg_attribute) as sum_xy,
 *           SUM(entity_avg_amount) as sum_x,
 *           SUM(entity_avg_attribute) as sum_y
 *         FROM base_metrics
 *       )
 *       -- Finally combine into correlation formula with qualified references
 *       SELECT 
 *         (stats.n * stats.sum_xy - stats.sum_x * stats.sum_y) /
 *         NULLIF(
 *           (SQRT(stats.n * SUM(POWER(bm.entity_avg_amount, 2)) - POWER(stats.sum_x, 2)) *
 *            SQRT(stats.n * SUM(POWER(bm.entity_avg_attribute, 2)) - POWER(stats.sum_y, 2))),
 *            0
 *         ) as correlation_coefficient
 *       FROM stats
 *       CROSS JOIN base_metrics bm;
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
 * 
 * 16. "column must appear in the GROUP BY clause or be used in an aggregate function"
 *     Problem: Using non-aggregated columns in grouped statistical calculations
 *     Solution: 
 *     - Pre-calculate all statistical components within groups
 *     - Carry grouping columns through all CTEs
 *     - Use only aggregated values in final calculations
 *     Example fix:
 *       Instead of:
 *         WITH stats AS (
 *           SELECT 
 *             segment,
 *             COUNT(*) as n,
 *             SUM(x*y) as sum_xy
 *           FROM data
 *           GROUP BY segment
 *         )
 *         SELECT 
 *           segment,
 *           (n * sum_xy - ...) / ...  -- Error: n not grouped
 *         FROM stats
 *       Use:
 *         WITH stats AS (
 *           SELECT 
 *             segment,
 *             COUNT(*) as segment_n,
 *             SUM(x*y) as segment_sum_xy,
 *             SUM(POWER(x, 2)) as segment_sum_x2
 *           FROM data
 *           GROUP BY segment
 *         )
 *         SELECT 
 *           segment,
 *           (segment_n * segment_sum_xy) / 
 *             SQRT(segment_n * segment_sum_x2)
 *         FROM stats
 *     Testing:
 *     - Verify all columns used in calculations are either:
 *       * Part of GROUP BY
 *       * Aggregated within group
 *     - Test with multiple grouping columns
 *     - Validate results match per-group manual calculations
 *     - Check edge cases with single-row groups
 */