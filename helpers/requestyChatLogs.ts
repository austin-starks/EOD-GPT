import mongoose, { Schema } from "mongoose";

export enum AiChatTypeEnum {
  Image = "image",
  Embeddings = "embeddings",
  Chat = "chat",
}

export interface IJson {
  name: string;
  description: string;
  parameters: any;
  strict?: boolean;
  additionalProperties?: false;
}

export interface ToolCall {
  id: string;
  type: "function";
  function: {
    name: string;
    arguments: string;
  };
}

export interface RequestyChatMessage {
  role: string;
  content: string | null;
  tool_calls?: ToolCall[];
}

export interface RequestyChatRequest {
  model: string;
  user: string;
  messages: RequestyChatMessage[];
  temperature: number;
  max_tokens?: number;
  tools?: Array<{ type: "function"; function: IJson }>;
  presence_penalty?: number;
  frequency_penalty?: number;
  response_format?: { type: "json_object" };
  seed?: number;
}

export interface RequestyUsage {
  prompt_tokens: number;
  completion_tokens: number;
  total_tokens: number;
}

export interface RequestyChoice {
  index: number;
  finish_reason: string;
  message: RequestyChatMessage;
}

export interface RequestyChatResponse {
  usage: RequestyUsage;
  choices: RequestyChoice[];
  created: number;
}

interface RequestyChatLog {
  type: AiChatTypeEnum;
  request: RequestyChatRequest;
  response: RequestyChatResponse;
  createdAt: Date;
  error?: string;
}

const RequestyChatLogSchema = new Schema({
  type: { type: String, required: true },
  request: { type: Object, required: true },
  response: { type: Object },
  error: { type: String },
  createdAt: { type: Date, default: Date.now },
});

const RequestyChatLogModel = mongoose.model<
  RequestyChatLog & mongoose.Document
>("RequestyChatLog", RequestyChatLogSchema);

class RequestyChatLog {
  static async getFullLogs() {
    return await RequestyChatLogModel.find();
  }

  static async logChat(
    request: RequestyChatRequest,
    response: RequestyChatResponse | null,
    error: string | null
  ) {
    if (error) {
      console.log("Requesty Chat Error: ", error);
    }
    const log = new RequestyChatLogModel({
      type: AiChatTypeEnum.Chat,
      request,
      response,
      error,
    });

    await log.save().catch((err) => console.log("Error saving log: ", err));
  }
}

export default RequestyChatLog;
