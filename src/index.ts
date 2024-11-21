import { PluginDefinition, setupPluginServer } from 'connery';
import chatWithYourPostgresqlDb from "./actions/chatWithYourPostgresqlDb.js";

const pluginDefinition: PluginDefinition = {
  name: 'PostgreSQL',
  description: 'Connery plugin to chat with a PostgreSQL database',
  actions: [chatWithYourPostgresqlDb],
};

const handler = await setupPluginServer(pluginDefinition);
export default handler;
