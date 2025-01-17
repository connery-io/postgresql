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
    WITH columns_info AS (
      SELECT
          c.table_schema,
          c.table_name,
          c.column_name,
          c.data_type,
          c.is_nullable,
          c.column_default,
          c.ordinal_position
      FROM
          information_schema.columns c
      WHERE
          c.table_schema NOT IN ('pg_catalog', 'information_schema')
    ),
    primary_keys AS (
      SELECT
          kcu.table_schema,
          kcu.table_name,
          kcu.column_name
      FROM
          information_schema.table_constraints tc
      JOIN
          information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
          AND tc.table_schema = kcu.table_schema
      WHERE
          tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_schema NOT IN ('pg_catalog', 'information_schema')
    ),
    foreign_keys AS (
      SELECT
          kcu.table_schema AS table_schema,
          kcu.table_name AS table_name,
          kcu.column_name AS column_name,
          ccu.table_schema AS foreign_table_schema,
          ccu.table_name AS foreign_table_name,
          ccu.column_name AS foreign_column_name
      FROM
          information_schema.table_constraints tc
      JOIN
          information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
          AND tc.table_schema = kcu.table_schema
      JOIN
          information_schema.constraint_column_usage ccu
          ON ccu.constraint_name = tc.constraint_name
          AND ccu.table_schema = tc.table_schema
      WHERE
          tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema NOT IN ('pg_catalog', 'information_schema')
    )
    SELECT
      jsonb_pretty(
          jsonb_agg(
              jsonb_build_object(
                  'table_schema', tbl.table_schema,
                  'table_name', tbl.table_name,
                  'columns', tbl.columns
              )
          )
      ) AS schema_json
    FROM (
      SELECT
          c.table_schema,
          c.table_name,
          jsonb_agg(
              jsonb_build_object(
                  'column_name', c.column_name,
                  'data_type', c.data_type,
                  'is_nullable', c.is_nullable,
                  'column_default', c.column_default,
                  'is_primary_key', CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END,
                  'is_foreign_key', CASE WHEN fk.column_name IS NOT NULL THEN true ELSE false END,
                  'foreign_table_schema', fk.foreign_table_schema,
                  'foreign_table_name', fk.foreign_table_name,
                  'foreign_column_name', fk.foreign_column_name
              ) ORDER BY c.ordinal_position
          ) AS columns
      FROM
          columns_info c
      LEFT JOIN
          primary_keys pk
          ON c.table_schema = pk.table_schema
          AND c.table_name = pk.table_name
          AND c.column_name = pk.column_name
      LEFT JOIN
          foreign_keys fk
          ON c.table_schema = fk.table_schema
          AND c.table_name = fk.table_name
          AND c.column_name = fk.column_name
      GROUP BY
          c.table_schema,
          c.table_name
      ORDER BY
          c.table_schema,
          c.table_name
    ) tbl;
  `;
  
  const schemaResult = await client.query(schemaQuery);
  
  const schemaJson = schemaResult.rows[0].schema_json;
  return schemaJson;
}

async function generateSqlQuery(apiKey: string, schemaInfo: string, question: string, maxRows: number): Promise<string> {
  const systemPrompt = `You are a PostgreSQL expert. Generate secure, read-only SQL queries based on natural language questions.
        Schema information: ${schemaInfo}
        
        Important: Return ONLY the raw SQL query without any formatting, markdown, or code blocks.
        
        Rules:
        - Use ONLY tables and columns that exist in the provided schema information
        - Do not make assumptions about columns that aren't explicitly listed in the schema
        - Generate only SELECT queries (no INSERT, UPDATE, DELETE, etc.)
        - Ensure queries are optimized for performance
        - Include relevant JOINs when needed
        - Add inline comments with -- to explain the query
        - Limit results to ${maxRows} rows using LIMIT clause
        - Use explicit column names instead of SELECT *
        - Add ORDER BY clauses when relevant
        - When using numeric calculations:
          * Cast numeric values explicitly (e.g., CAST(value AS NUMERIC))
          * Use ROUND(CAST(value AS NUMERIC), 2) for decimal places
          * Handle NULL values with COALESCE
        - When combining results (top/bottom rankings):
          * Use WITH clauses for better readability
          * Ensure column names and types match in UNION queries
          * Add labels/indicators to distinguish top vs bottom results
          * Use row_number() for rankings when needed
        - Do not include markdown code blocks or SQL syntax highlighting in your response
        - Do not include any other text in your response
        - If you cannot construct a query using only the available columns, respond with an error message starting with "ERROR:"`;

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