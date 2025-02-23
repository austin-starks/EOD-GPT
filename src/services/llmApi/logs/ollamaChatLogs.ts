import mongoose, { Schema } from "mongoose";

import { AiChatTypeEnum } from "./requestyChatLogs";

export interface OllamaChatMessage {
  role: string;
  content: string;
}

export interface OllamaChatRequest {
  model: string;
  messages: OllamaChatMessage[];
  stream: boolean;
  options?: {
    num_ctx?: number;
  };
}

export interface OllamaChatResponse {
  message: {
    role: string;
    content: string;
  };
  done: boolean;
  total_duration?: number;
  load_duration?: number;
  prompt_eval_duration?: number;
}

interface OllamaChatLog {
  type: AiChatTypeEnum;
  request: OllamaChatRequest;
  response: OllamaChatResponse;
  createdAt: Date;
  error?: string;
}

const OllamaChatLogSchema = new Schema({
  type: { type: String, required: true },
  request: { type: Object, required: true },
  response: { type: Object },
  error: { type: String },
  createdAt: { type: Date, default: Date.now },
});

const OllamaChatLogModel = mongoose.model<OllamaChatLog & mongoose.Document>(
  "OllamaChatLog",
  OllamaChatLogSchema
);

class OllamaChatLog {
  static async getFullLogs() {
    return await OllamaChatLogModel.find();
  }

  static async logChat(
    request: OllamaChatRequest,
    response: OllamaChatResponse | null,
    error: string | null
  ) {
    if (error) {
      console.log("Ollama Chat Error: ", error);
    }
    const log = new OllamaChatLogModel({
      type: AiChatTypeEnum.Chat,
      request,
      response,
      error,
    });

    await log.save().catch((err) => console.log("Error saving log: ", err));
  }
}

export default OllamaChatLog;
