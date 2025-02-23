import RequestyChatLog, {
  RequestyChatMessage,
  RequestyChatRequest,
  RequestyChatResponse,
} from "./requestyChatLogs";

import axios from "axios";
import dotenv from "dotenv";

dotenv.config();

export interface OpenRouterModelInfo {
  id: string;
  name: string;
  created: number;
  description: string;
  pricing: {
    prompt: string;
    completion: string;
    request: string;
    image: string;
  };
  context_length: number;
  architecture: {
    tokenizer: string;
    instruct_type: string;
    modality: string;
  };
  top_provider: {
    max_completion_tokens: number;
    is_moderated: boolean;
  };
  per_request_limits: {
    prompt_tokens: string;
    completion_tokens: string;
  };
}
export enum RequestyModelEnum {
  o3Mini = "openai/o3-mini",
  geminiFlash2 = "google/gemini-2.0-flash-001",
}

export interface ChatMessage {
  _id?: string;
  sender: "AI Assistant" | "User" | "System";
  content: string;
}

export interface SendChatMessageRequest {
  systemPrompt: string;
  model: RequestyModelEnum;
  temperature: number;
  messages: ChatMessage[];
  userId: string;
}

export interface OpenRouterListModelsResponse {
  data: OpenRouterModelInfo[];
}
export const transformSenderToRole = (
  message: ChatMessage
): ChatMessageWithRole => {
  const sender = message.sender.toLowerCase().trim();
  const role =
    sender === "ai assistant" || sender === "assistant"
      ? "assistant"
      : sender === "system"
      ? "system"
      : "user";
  return { role, content: message?.content || "" };
};

export const enforceAlternationAndAddContent = (
  messages: ChatMessageWithRole[]
): ChatMessageWithRole[] => {
  const newMessages: ChatMessageWithRole[] = [];
  let lastRole: "user" | "assistant" | "system" | null = null;
  for (let i = 0; i < messages.length; i++) {
    if (messages[i].role === lastRole) {
      newMessages.push({
        content:
          "This is a placeholder message to enforce the User/AI Assistant pattern. Do not let this distract you from the user request",
        role: lastRole === "user" ? "assistant" : "user",
      });
    }
    messages[i].content =
      messages[i]?.content?.trim() ||
      "This is a placeholder message to enforce the User/AI Assistant pattern. Do not let this distract you from the user request";
    newMessages.push(messages[i]);
    lastRole = messages[i].role;
  }
  return newMessages;
};

export interface ChatMessageWithRole {
  role: "user" | "assistant" | "system";
  content: string;
}

class RequestyServiceClient {
  baseUrl: string = "https://router.requesty.ai/v1/chat/completions";
  private static modelsCache: string[] | null = null;
  private static lastCacheTime: number | null = null;
  private static CACHE_DURATION_FOR_MODELS_MS = 12 * 60 * 60 * 1000; // 12 hours in milliseconds

  public fallbackModels = [
    {
      model: null,
      maxAttempts: 2,
    },
  ];
  private static readonly RETRY_DELAY_MS = 1000; // 1 second

  async getModels(): Promise<string[]> {
    // Check if cache exists and is still valid
    if (
      RequestyServiceClient.modelsCache &&
      RequestyServiceClient.lastCacheTime &&
      Date.now() - RequestyServiceClient.lastCacheTime <
        RequestyServiceClient.CACHE_DURATION_FOR_MODELS_MS
    ) {
      return RequestyServiceClient.modelsCache;
    }

    const apiKey = process.env.REQUESTY_API_KEY;
    if (!apiKey) {
      throw new Error("API key for Requesty is missing");
    }

    const headers = {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    };

    try {
      const response = await axios.get<OpenRouterListModelsResponse>(
        "https://router.requesty.ai/v1/models",
        { headers }
      );

      // Update cache with both original and :online versions
      RequestyServiceClient.modelsCache = response.data.data.flatMap((m) => [
        m.id,
        `${m.id}:online`,
      ]);
      RequestyServiceClient.lastCacheTime = Date.now();

      return RequestyServiceClient.modelsCache;
    } catch (error: any) {
      console.error("Error fetching model list from Requesty:", error.message);
      throw new Error("Failed to list models from Requesty");
    }
  }

  async sendRequest(
    request: SendChatMessageRequest
  ): Promise<RequestyChatResponse> {
    let { model } = request;

    // Try original model first
    try {
      const response = await this.tryRequest(model, request);
      return response;
    } catch (error: any) {
      console.error(`Original model ${model} failed:`, error.message);
    }
    const isOnlineModel = model.endsWith(":online");
    // Try fallback models
    for (const fallback of this.fallbackModels) {
      const fallbackModel = fallback.model
        ? isOnlineModel
          ? `${fallback.model}:online`
          : fallback.model
        : model;

      for (let attempt = 0; attempt < fallback.maxAttempts; attempt++) {
        try {
          const response = await this.tryRequest(fallbackModel, request);
          return response;
        } catch (error: any) {
          console.error(
            `Fallback model ${fallbackModel} attempt ${attempt + 1} failed:`,
            error.message
          );
          await new Promise((resolve) =>
            setTimeout(resolve, RequestyServiceClient.RETRY_DELAY_MS)
          );
        }
      }
    }

    throw new Error(
      "All models and attempts exhausted. Please try again later."
    );
  }

  // Helper method to reduce code duplication
  private async tryRequest(model: string, request: SendChatMessageRequest) {
    const { systemPrompt, temperature, messages, userId } = request;
    const systemPromptMessage = {
      sender: "System" as "System",
      content: systemPrompt,
    };

    const openRouterResponse = await this.submitRequest(
      [systemPromptMessage, ...messages],
      userId,
      model,
      temperature
    );

    return openRouterResponse;
  }

  private transformMessagesForModel(
    messages: ChatMessageWithRole[],
    model: string
  ): ChatMessageWithRole[] {
    if (model.includes(RequestyModelEnum.o3Mini)) {
      // add a placeholder message at index 1 if messages[1].role is assistant
      if (messages[1] && messages[1].role === "assistant") {
        messages.splice(1, 0, {
          role: "user",
          content:
            "The below message is an intro message from the AI assistant. It may or may not be relevant to the user's goal. Please keep that in mind.",
        });
      }
      messages = enforceAlternationAndAddContent(messages);
      if (messages[messages.length - 1].role === "assistant") {
        messages.push({
          role: "user",
          content: "Read the above messages and respond.",
        });
      }
    }
    // if its an online model, we should append to the last user message that we should not mention the web search in the response
    if (model.includes(":online")) {
      if (messages[messages.length - 1].role === "user") {
        messages[messages.length - 1].content +=
          "\n\nDo not mention the web search in the response.";
      }
    }
    return messages;
  }

  private async submitRequest(
    messages: ChatMessage[],
    userId: string,
    model: string,
    temperature: number
  ): Promise<RequestyChatResponse> {
    let formattedMessages = messages.map(transformSenderToRole);

    return await this.chat(formattedMessages, userId, model, temperature);
  }

  getError(response: any): string | undefined {
    let error = (response as any).data?.error;
    if (error) {
      return JSON.stringify(error, null, 2);
    }
    return undefined;
  }

  private async chat(
    messages: RequestyChatMessage[],
    userId: string,
    model: string,
    temperature: number
  ): Promise<RequestyChatResponse> {
    const apiKey = process.env.REQUESTY_API_KEY;
    if (!apiKey) {
      throw new Error("API key for Requesty is missing");
    }

    // Prepare request payload
    messages = this.transformMessagesForModel(
      messages as ChatMessageWithRole[],
      model
    );

    const requestPayload: RequestyChatRequest = {
      model,
      user: userId,
      messages,
      temperature,
    };

    const headers = {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    };

    const response = await axios.post<RequestyChatResponse>(
      this.baseUrl,
      requestPayload,
      { headers }
    );

    // Check for API-level errors
    const error = this.getError(response);
    if (error) throw new Error(error);

    // Log and return the response
    await RequestyChatLog.logChat(requestPayload, response.data, null);
    return response.data;
  }
}

export default RequestyServiceClient;
