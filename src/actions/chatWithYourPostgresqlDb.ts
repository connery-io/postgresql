import { ActionDefinition, ActionContext, OutputObject } from 'connery';
import pkg from 'pg';
const { Client } = pkg;
import OpenAI from 'openai';

const actionDefinition: ActionDefinition = {
  key: 'chatWithYourPostgresqlDb',
  name: 'Chat with your PostgreSQL DB',
  description: 'Users can send DB requests in natural language and receive data and/or helpful feedback.',
  type: 'read',
  inputParameters: [
    {
      key: 'openaiApiKey',
      name: 'OpenAI API Key',
      description: 'Your OpenAI API key',
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
      key: 'schema',
      name: 'Database Schema',
      description: 'Description of your database schema including table relationships and column descriptions',
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
    }
  ],
  operation: {
    handler: handler,
  },
  outputParameters: [
    {
      key: 'response',
      name: 'Response',
      description: 'The answer to your database question',
      type: 'string',
      validation: {
        required: true,
      },
    }
  ],
};

export default actionDefinition;

export async function handler({ input }: ActionContext): Promise<OutputObject> {
  const client = new Client(input.connectionString);
  const maxRows = parseInt(input.maxRows || '100');

  try {
    await client.connect();
    const schemaInfo = await getSchemaInfo(client, input.schema);
    const sqlQuery = await generateSqlQuery(input.openaiApiKey, schemaInfo, input.question, maxRows);
    const sanitizedQuery = sanitizeSqlQuery(sqlQuery);
    
    //console.log('Original SQL Query:', sqlQuery);
    //console.log('Sanitized SQL Query:', sanitizedQuery);
    
    const result = await client.query(sanitizedQuery);
    return formatResponse(result.rows, input.instructions);
  } catch (error: unknown) {
    throw new Error(`Database error: ${error instanceof Error ? error.message : String(error)}`);
  } finally {
    await client.end();
  }
}

async function getSchemaInfo(client: pkg.Client, providedSchema?: string): Promise<string> {
  if (providedSchema) return providedSchema;

  const schemaQuery = `
    SELECT 
      table_name,
      column_name,
      data_type,
      column_description
    FROM 
      information_schema.columns c
    LEFT JOIN 
      pg_description d ON 
      d.objoid = (quote_ident(c.table_schema)||'.'||quote_ident(c.table_name))::regclass AND 
      d.objsubid = c.ordinal_position
    WHERE 
      table_schema = 'public'
    ORDER BY 
      table_name, ordinal_position;
  `;
  const schemaResult = await client.query(schemaQuery);
  return JSON.stringify(schemaResult.rows, null, 2);
}

async function generateSqlQuery(apiKey: string, schemaInfo: string, question: string, maxRows: number): Promise<string> {
  const ai = new OpenAI({ apiKey });
  const completion = await ai.chat.completions.create({
    model: "gpt-4",
    messages: [
      {
        role: "system",
        content: `You are a PostgreSQL expert. Generate secure, read-only SQL queries based on natural language questions.
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
        - Do not include markdown code blocks or SQL syntax highlighting in your response
        - Do not include any other text in your response
        - If you cannot construct a query using only the available columns, respond with an error message starting with "ERROR:"`
      },
      { role: "user", content: question }
    ],
    temperature: 0
  });

  const sqlQuery = completion.choices[0]?.message?.content?.trim();
  if (!sqlQuery) {
    throw new Error('Failed to generate SQL query: No response from OpenAI');
  }

  if (sqlQuery.startsWith('ERROR:')) {
    throw new Error(sqlQuery);
  }

  return sqlQuery;
}

function sanitizeSqlQuery(query: string): string {
  return query
    .replace(/```sql/gi, '')
    .replace(/```/g, '')
    .replace(/--.*$/gm, '')
    .replace(/^\s*[\r\n]/gm, '')
    .replace(/^(?!SELECT\s)/i, 'SELECT ')
    .replace(/;[\s\S]*$/, ';')
    .replace(/[^;]$/, match => match + ';')
    .trim();
}

function formatResponse(rows: any[], instructions?: string): OutputObject {
  let response = JSON.stringify(rows, null, 2);
  if (instructions) {
    response = `Instructions for the following content: ${instructions}\n\n${response}`;
  }
  return { response };
}
