import { PluginDefinition, setupPluginServer } from 'connery';
import chatWithYourDb from "./actions/chatWithYourDb.js";
import updateRecordWithGenAI from "./actions/updateAnyFieldOfRecordWithAI.js";
import updateSpecificFieldOfRecord from "./actions/updateSpecificFieldOfRecord.js";

const pluginDefinition: PluginDefinition = {
  name: 'PostgreSQL',
  description: 'Connery plugin to chat with a PostgreSQL database',
  actions: [chatWithYourDb, updateRecordWithGenAI, updateSpecificFieldOfRecord],
};

const handler = await setupPluginServer(pluginDefinition);
export default handler;
