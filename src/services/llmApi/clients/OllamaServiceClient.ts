import { ChatMessage, SendChatMessageRequest } from "./RequestyServiceClient";
import OllamaChatLog, {
  OllamaChatRequest,
  OllamaChatResponse,
} from "../logs/ollamaChatLogs";

import axios from "axios";
import dotenv from "dotenv";

dotenv.config();

export enum OllamaModelEnum {
  deepseekR1 = "deepseek-r1:32b",
  llama3 = "llama3.3:latest",
  deepscaler = "deepscaler",
}

class OllamaServiceClient {
  baseUrl: string;

  constructor() {
    const ollamaUrl = process.env.OLLAMA_SERVICE_URL;
    if (!ollamaUrl) {
      throw new Error("OLLAMA_SERVICE_URL environment variable is not set");
    }
    this.baseUrl = `${ollamaUrl}/api/chat`;
  }

  private transformSenderToRole(message: ChatMessage) {
    const sender = message.sender.toLowerCase().trim();
    const role =
      sender === "ai assistant" || sender === "assistant"
        ? "assistant"
        : sender === "system"
        ? "system"
        : "user";
    return { role, content: message?.content || "" };
  }

  getChatResponseContent(response: OllamaChatResponse) {
    return response.message.content;
  }

  getModel() {
    return OllamaModelEnum.deepseekR1;
  }

  async sendRequest(
    request: SendChatMessageRequest
  ): Promise<OllamaChatResponse> {
    const { systemPrompt, messages, model } = request;

    const formattedMessages = [
      { sender: "System" as const, content: systemPrompt },
      ...messages,
    ].map((msg) => this.transformSenderToRole(msg));

    const chatRequest: OllamaChatRequest = {
      model,
      messages: formattedMessages,
      stream: false,
      options: {
        num_ctx: 32768,
      },
    };

    try {
      const response = await axios.post<OllamaChatResponse>(
        this.baseUrl,
        chatRequest
      );

      await OllamaChatLog.logChat(chatRequest, response.data, null);
      return response.data;
    } catch (error: any) {
      await OllamaChatLog.logChat(chatRequest, null, error.message);
      throw new Error(`Error in chat function: ${error.message}`);
    }
  }
}

export default OllamaServiceClient;
