# Database Agent (PostgreSQL)

AI agent that helps you to chat with and selectively update a PostgreSQL database.

## Actions

This plugin provides three actions for interacting with PostgreSQL databases:

1. **READ: Chat with your DB** (`chatWithYourDb`)

   - Executes a read-only SQL query and returns the results
   - Requires read-only connection string and SQL query as inputs
   - Automatically retrieves the schema of the database and uses it to generate the SQL query
   - Perfect for SELECT statements and data retrieval for non-technical users

2. **WRITE: Update Specific Field or Record** (`updateSpecificFieldOfRecord`)

   - Updates a specific field in a database record using a predefined query template
   - Requires write-enabled connection string, update query template with {record} and {value} placeholders
   - Supports transaction handling with automatic rollback on failure
   - Returns both the executed query and the updated record

## Repository structure

This repository contains the plugin's source code.

| Path                            | Description                                                                                                                                          |
| ------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| [./src/index.ts](/src/index.ts) | **The entry point for the plugin.** It contains the plugin definition and references to all the actions.                                             |
| [./src/actions/](/src/actions/) | **This folder contains all the actions of the plugin.** Each action is represented by a separate file with the action definition and implementation. |

## Built using Connery SDK

This plugin is built using [Connery SDK](https://github.com/connery-io/connery-sdk), the open-source SDK for creating AI plugins and actions.

[Learn how to use the plugin and its actions.](https://docs.connery.io/sdk/guides/use-a-plugin)

## Support

If you have any questions or need help with this plugin, please create an issue in this repository.
