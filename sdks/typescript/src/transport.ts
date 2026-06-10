import type {
  AgentCard,
  CancelTaskSuccessResponse,
  DeleteTaskPushNotificationConfigParams,
  GetAuthenticatedExtendedCardSuccessResponse,
  GetTaskPushNotificationConfigParams,
  GetTaskPushNotificationConfigSuccessResponse,
  GetTaskSuccessResponse,
  JSONRPCRequest,
  JSONRPCResponse,
  ListTaskPushNotificationConfigParams,
  ListTaskPushNotificationConfigSuccessResponse,
  Message,
  MessageSendParams,
  SendMessageSuccessResponse,
  SetTaskPushNotificationConfigSuccessResponse,
  Task,
  TaskArtifactUpdateEvent,
  TaskIdParams,
  TaskPushNotificationConfig,
  TaskQueryParams,
  TaskStatusUpdateEvent,
} from '@a2a-js/sdk';
import type { Transport, TransportFactory, RequestOptions } from '@a2a-js/sdk/client';
import { createInbox, type NatsConnection, type Subscription } from '@nats-io/transport-node';

import { NATS_TRANSPORT_PROTOCOL_NAME, natsSubjectFromAgentUrl, type A2ANatsStreamFrame } from '../../../protocol/src/index.js';
import { decodeJson, decodeStreamFrame, encodeJson, encodeRequestFrame } from './codec.js';
import { mapJsonRpcError } from './errors.js';

type A2AStreamEventData = Message | Task | TaskStatusUpdateEvent | TaskArtifactUpdateEvent;
type SendMessageResult = Message | Task;

export interface NatsMsgLike {
  readonly data: Uint8Array;
  readonly reply?: string;
  respond?(data?: Uint8Array): boolean;
}

export interface NatsRequestOptions {
  readonly timeout?: number;
  readonly signal?: AbortSignal;
}

export interface NatsConnectionLike {
  publish(subject: string, data?: Uint8Array, options?: { reply?: string }): void;
  request(subject: string, data?: Uint8Array, options?: NatsRequestOptions): Promise<NatsMsgLike>;
  subscribe(subject: string, options?: { queue?: string }): AsyncIterable<NatsMsgLike> & { unsubscribe(): void };
  flush?(): Promise<void>;
}

export interface NatsTransportFactoryOptions {
  readonly connection: NatsConnection | NatsConnectionLike;
  readonly requestTimeoutMs?: number;
  readonly createInbox?: () => string;
}

export class NatsTransportFactory implements TransportFactory {
  constructor(private readonly options: NatsTransportFactoryOptions) {}

  get protocolName(): string {
    return NATS_TRANSPORT_PROTOCOL_NAME;
  }

  async create(url: string, _agentCard: AgentCard): Promise<Transport> {
    return new NatsA2AClientTransport({
      ...this.options,
      subject: natsSubjectFromAgentUrl(url),
    });
  }
}

export function createNatsTransportFactory(options: NatsTransportFactoryOptions): NatsTransportFactory {
  return new NatsTransportFactory(options);
}

export interface NatsA2AClientTransportOptions extends NatsTransportFactoryOptions {
  readonly subject: string;
}

export class NatsA2AClientTransport implements Transport {
  private requestIdCounter = 1;
  private readonly requestTimeoutMs: number;
  private readonly createInbox: () => string;

  constructor(private readonly options: NatsA2AClientTransportOptions) {
    this.requestTimeoutMs = options.requestTimeoutMs ?? 30_000;
    this.createInbox = options.createInbox ?? (() => createInbox());
  }

  async getExtendedAgentCard(options?: RequestOptions): Promise<AgentCard> {
    const response = await this.sendRpcRequest<undefined, GetAuthenticatedExtendedCardSuccessResponse>(
      'agent/getAuthenticatedExtendedCard',
      undefined,
      options
    );
    return response.result;
  }

  async sendMessage(params: MessageSendParams, options?: RequestOptions): Promise<SendMessageResult> {
    const response = await this.sendRpcRequest<MessageSendParams, SendMessageSuccessResponse>(
      'message/send',
      params,
      options
    );
    return response.result;
  }

  async *sendMessageStream(
    params: MessageSendParams,
    options?: RequestOptions
  ): AsyncGenerator<A2AStreamEventData, void, undefined> {
    yield* this.sendStreamingRequest<MessageSendParams>('message/stream', params, options);
  }

  async setTaskPushNotificationConfig(
    params: TaskPushNotificationConfig,
    options?: RequestOptions
  ): Promise<TaskPushNotificationConfig> {
    const response = await this.sendRpcRequest<
      TaskPushNotificationConfig,
      SetTaskPushNotificationConfigSuccessResponse
    >('tasks/pushNotificationConfig/set', params, options);
    return response.result;
  }

  async getTaskPushNotificationConfig(
    params: GetTaskPushNotificationConfigParams,
    options?: RequestOptions
  ): Promise<TaskPushNotificationConfig> {
    const response = await this.sendRpcRequest<
      GetTaskPushNotificationConfigParams,
      GetTaskPushNotificationConfigSuccessResponse
    >('tasks/pushNotificationConfig/get', params, options);
    return response.result;
  }

  async listTaskPushNotificationConfig(
    params: ListTaskPushNotificationConfigParams,
    options?: RequestOptions
  ): Promise<TaskPushNotificationConfig[]> {
    const response = await this.sendRpcRequest<
      ListTaskPushNotificationConfigParams,
      ListTaskPushNotificationConfigSuccessResponse
    >('tasks/pushNotificationConfig/list', params, options);
    return response.result;
  }

  async deleteTaskPushNotificationConfig(
    params: DeleteTaskPushNotificationConfigParams,
    options?: RequestOptions
  ): Promise<void> {
    await this.sendRpcRequest<DeleteTaskPushNotificationConfigParams, JSONRPCResponse>(
      'tasks/pushNotificationConfig/delete',
      params,
      options
    );
  }

  async getTask(params: TaskQueryParams, options?: RequestOptions): Promise<Task> {
    const response = await this.sendRpcRequest<TaskQueryParams, GetTaskSuccessResponse>(
      'tasks/get',
      params,
      options
    );
    return response.result;
  }

  async cancelTask(params: TaskIdParams, options?: RequestOptions): Promise<Task> {
    const response = await this.sendRpcRequest<TaskIdParams, CancelTaskSuccessResponse>(
      'tasks/cancel',
      params,
      options
    );
    return response.result;
  }

  async *resubscribeTask(
    params: TaskIdParams,
    options?: RequestOptions
  ): AsyncGenerator<A2AStreamEventData, void, undefined> {
    yield* this.sendStreamingRequest<TaskIdParams>('tasks/resubscribe', params, options);
  }

  private async sendRpcRequest<TParams, TResponse extends JSONRPCResponse>(
    method: string,
    params: TParams,
    options?: RequestOptions
  ): Promise<TResponse> {
    const request = this.createRequest(method, params);
    const message = await this.options.connection.request(
      this.options.subject,
      encodeRequestFrame(request, options?.serviceParameters),
      {
        timeout: this.requestTimeoutMs,
        signal: options?.signal,
      }
    );
    const response = decodeJson(message.data) as JSONRPCResponse;
    this.assertResponseId(response, request.id, method);
    if ('error' in response) {
      throw mapJsonRpcError(response);
    }
    return response as TResponse;
  }

  private async *sendStreamingRequest<TParams>(
    method: string,
    params: TParams,
    options?: RequestOptions
  ): AsyncGenerator<A2AStreamEventData, void, undefined> {
    const request = this.createRequest(method, params);
    const reply = this.createInbox();
    const subscription = this.options.connection.subscribe(reply) as Subscription | (AsyncIterable<NatsMsgLike> & { unsubscribe(): void });
    const abort = () => subscription.unsubscribe();

    try {
      options?.signal?.addEventListener('abort', abort, { once: true });
      await this.options.connection.flush?.();
      this.options.connection.publish(
        this.options.subject,
        encodeRequestFrame(request, options?.serviceParameters),
        { reply }
      );

      for await (const message of subscription) {
        const frame = decodeStreamFrame(message.data);
        if (frame.kind === 'complete') {
          if (frame.id !== request.id) {
            throw new Error(`NATS stream completion ID mismatch for ${method}. Expected ${request.id}, got ${frame.id}.`);
          }
          return;
        }
        if (frame.kind === 'transport-error') {
          throw new Error(frame.message);
        }

        const response = this.responseFromFrame(frame);
        this.assertResponseId(response, request.id, method);
        if ('error' in response) {
          throw mapJsonRpcError(response);
        }
        if (!('result' in response)) {
          throw new Error(`NATS stream response for ${method} did not contain a result.`);
        }
        yield response.result as A2AStreamEventData;
      }
    } finally {
      options?.signal?.removeEventListener('abort', abort);
      subscription.unsubscribe();
    }
  }

  private createRequest<TParams>(method: string, params: TParams): JSONRPCRequest {
    return {
      jsonrpc: '2.0',
      method,
      params: params as JSONRPCRequest['params'],
      id: this.requestIdCounter++,
    };
  }

  private responseFromFrame(frame: A2ANatsStreamFrame): JSONRPCResponse {
    if (frame.kind !== 'response') {
      throw new Error(`Unexpected A2A NATS stream frame: ${frame.kind}`);
    }
    return frame.response;
  }

  private assertResponseId(response: JSONRPCResponse, requestId: JSONRPCRequest['id'], method: string): void {
    if (response.id !== requestId) {
      throw new Error(`JSON-RPC response ID mismatch for ${method}. Expected ${requestId}, got ${response.id}.`);
    }
  }
}
