import { ActionDefinition, ActionContext, OutputObject } from 'connery';
import pkg from 'pg';
const { Client } = pkg;
import { Anthropic } from '@anthropic-ai/sdk';

const actionDefinition: ActionDefinition = {
  key: 'updateAnyFieldOfRecordWithAI',
  name: 'Update any Field of Record with AI',
  description: 'Update a single field in a specific database record using natural language, where AI generates the SQL automatically.',
  type: 'update',
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
      description: 'PostgreSQL connection string (requires write permissions)',
      type: 'string',
      validation: {
        required: true,
      },
    },
    {
      key: 'updateRequest',
      name: 'Update Request',
      description: 'Explicit description of record, field, and new value to update',
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
    }
  ],
  operation: {
    handler: handler,
  },
  outputParameters: [
    {
      key: 'updatedRecord',
      name: 'Updated Record',
      description: 'The updated database record',
      type: 'string',
      validation: {
        required: true,
      },
    },
    {
      key: 'query',
      name: 'Query',
      description: 'The executed SQL update query',
      type: 'string',
      validation: {
        required: true,
      },
    }
  ],
};

export default actionDefinition;

export async function handler({ input }: ActionContext): Promise<OutputObject> {
  let client: pkg.Client | null = null;

  try {
    client = new Client(input.connectionString);
    await validateAndConnect(client, input.connectionString);
    
    const schemaInfo = await getSchemaInfo(client);
    const updateQuery = await generateUpdateQuery(input.anthropicApiKey, schemaInfo, input.updateRequest);
    const result = await executeTransaction(client, updateQuery);
    
    return {
      updatedRecord: formatRecordResponse(result, input.instructions),
      query: formatQueryResponse(updateQuery)
    };
  } catch (error) {
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

async function validateAndConnect(client: pkg.Client, connectionString: string): Promise<void> {
  const debugConnectionString = connectionString.replace(/:([^@]+)@/, ':****@');

  if (!connectionString.includes('postgresql://')) {
    throw new Error(
      'Invalid connection string format. Connection string must be in the format:\n' +
      'postgresql://[user]:[password]@[host]:[port]/[database]\n' +
      'Example: postgresql://myuser:mypassword@localhost:5432/mydatabase\n\n' +
      'Your provided string format: ' + debugConnectionString
    );
  }

  try {
    const urlParts = new URL(connectionString);
    if (!urlParts.host || !urlParts.pathname.slice(1)) {
      throw new Error(
        'Connection string is missing required components.\n' +
        'Required: host and database name\n' +
        'Current string format: ' + debugConnectionString
      );
    }
  } catch (urlError) {
    throw new Error(
      'Connection string is malformed.\n' +
      'Required format: postgresql://[user]:[password]@[host]:[port]/[database]\n' +
      'Your provided string: ' + debugConnectionString
    );
  }

  try {
    await client.connect();
    await client.query('SELECT 1');
  } catch (error) {
    throw new Error(`Database connection failed: ${error instanceof Error ? error.message : String(error)}`);
  }
}

async function executeTransaction(client: pkg.Client, updateQuery: string): Promise<any> {
  try {
    await client.query('BEGIN');

    const queries = updateQuery.split(';').filter(q => q.trim());
    if (queries.length !== 2) {
      throw new Error('Invalid query generated: Expected UPDATE and SELECT statements');
    }

    await client.query(queries[0]); // Execute UPDATE
    const result = await client.query(queries[1]); // Execute SELECT

    await client.query('COMMIT');
    return result;
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  }
}

function formatRecordResponse(result: any, instructions?: string): string {
  let response = 'Update successful. Modified record:\n';
  response += JSON.stringify(result.rows[0], null, 2);

  if (instructions) {
    response = `Instructions for the following content: ${instructions}\n\n${response}`;
  }

  return response;
}

function formatQueryResponse(updateQuery: string): string {
  return updateQuery;
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
  return schemaResult.rows[0].schema_json;
}

async function generateUpdateQuery(apiKey: string, schemaInfo: string, updateRequest: string): Promise<string> {
  const systemPrompt = `You are a PostgreSQL expert. Generate secure UPDATE queries based on natural language requests.
        Schema information: ${schemaInfo}
        
        IMPORTANT: You must ALWAYS return exactly two statements:
        1. An UPDATE statement that modifies a single record
        2. A SELECT statement that returns the modified record
        
        Rules:
        - Use ONLY tables and columns that exist in the provided schema information
        - Generate only UPDATE queries that modify a single record
        - ALWAYS include a WHERE clause that precisely identifies one record
        - Do NOT use parameterized queries ($1, $2) - use actual values instead
        - The WHERE clause in the SELECT must match the WHERE clause in the UPDATE
        - Return only the raw SQL statements, no explanations
        - If you cannot construct a safe query, respond with "ERROR:"

        Example response format:
        UPDATE users SET email = 'john@example.com' WHERE id = 5;
        SELECT * FROM users WHERE id = 5;`;

  const ai = new Anthropic({ apiKey });
  const completion = await ai.messages.create({
    model: "claude-3-5-sonnet-20241022",
    max_tokens: 8192,
    messages: [
      {
        role: "user",
        content: systemPrompt + "\n\n" + updateRequest
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

  // Verify that we have both UPDATE and SELECT statements
  const statements = sqlQuery.split(';').filter(q => q.trim());
  if (statements.length !== 2 || !statements[0].trim().toUpperCase().startsWith('UPDATE') || 
      !statements[1].trim().toUpperCase().startsWith('SELECT')) {
    throw new Error('Invalid query format: Must include both UPDATE and SELECT statements');
  }

  return sqlQuery;
}
