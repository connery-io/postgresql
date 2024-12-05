import { ActionDefinition, ActionContext, OutputObject } from 'connery';
import pkg from 'pg';
const { Client } = pkg;

const actionDefinition: ActionDefinition = {
  key: 'updateSpecificFieldOfRecord',
  name: 'Update Specific Field of Record',
  description: 'Updates a specific field in a database record using a predefined query template.',
  type: 'update',
  inputParameters: [
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
      key: 'updateQuery',
      name: 'Update Query Template',
      description: 'SQL UPDATE query template with {record} and {value} placeholders',
      type: 'string',
      validation: {
        required: true,
      },
    },
    {
      key: 'record',
      name: 'Record Identifier',
      description: 'Value to replace {record} placeholder in the query',
      type: 'string',
      validation: {
        required: true,
      },
    },
    {
      key: 'updateValue',
      name: 'Update Value',
      description: 'New value to replace {new_value} placeholder in the query',
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
    
    const updateQuery = constructUpdateQuery(
      input.updateQuery,
      input.record,
      input.updateValue
    );
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

function constructUpdateQuery(queryTemplate: string, record: string, value: string): string {
  // Validate that the template contains both placeholders
  if (!queryTemplate.includes('{record}') || !queryTemplate.includes('{value}')) {
    throw new Error('Update query template must contain both {record} and {value} placeholders');
  }

  // Use String.prototype.replace with a replacer function to preserve case
  const updateStatement = queryTemplate
    .replace(/{record}/g, (match) => `'${record}'`) // Preserve case of record
    .replace(/{value}/g, (match) => `'${value}'`);  // Preserve case of value

  // Extract the WHERE clause to use in the SELECT statement
  const whereClauseMatch = updateStatement.match(/WHERE\s+(.+)$/i);
  if (!whereClauseMatch) {
    throw new Error('Update query template must contain a WHERE clause');
  }

  const whereClause = whereClauseMatch[1];
  const tableName = updateStatement.match(/UPDATE\s+([^\s]+)/i)?.[1];
  
  if (!tableName) {
    throw new Error('Invalid update query template format');
  }

  const selectStatement = `SELECT * FROM ${tableName} WHERE ${whereClause}`;
  return `${updateStatement};\n${selectStatement};`;
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
