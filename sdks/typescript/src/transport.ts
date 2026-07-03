import {
  A2A_PROTOCOL_VERSION,
  A2A_VERSION_HEADER,
  AgentCard,
  CancelTaskRequest,
  DeleteTaskPushNotificationConfigRequest,
  GetExtendedAgentCardRequest,
  GetTaskPushNotificationConfigRequest,
  GetTaskRequest,
  ListTaskPushNotificationConfigsRequest,
  ListTaskPushNotificationConfigsResponse,
  ListTasksRequest,
  ListTasksResponse,
  StreamResponse,
  SubscribeToTaskRequest,
  Task,
  TaskPushNotificationConfig,
  type SendMessageRequest,
  type SendMessageResponse,
  type SendMessageResult,
} from '@a2a-js/sdk';
import { createInbox, headers, type MsgHdrs, type NatsConnection, type Subscription } from '@nats-io/transport-node';
import type { RequestOptions, Transport, TransportFactory } from '@a2a-js/sdk/client';

import {
  NATS_TRANSPORT_PROTOCOL_NAME,
  natsSubjectFromAgentUrl,
  type A2ANatsStreamFrame,
  type A2ANatsTransportMethod,
} from '../../../protocol/src/index.js';
import {
  decodeResponsePayload,
  decodeStreamFrame,
  decodeUnaryFrame,
  encodeRequestFrame,
  resultFromSendMessageResponse,
} from './codec.js';
import { framePayloadToError } from './errors.js';

export interface NatsMsgLike {
  readonly data: Uint8Array;
  readonly reply?: string;
  readonly headers?: MsgHdrs;
  respond?(data?: Uint8Array, options?: { headers?: MsgHdrs }): boolean;
}

export interface NatsRequestOptions {
  readonly timeout?: number;
  readonly signal?: AbortSignal;
  readonly headers?: MsgHdrs;
}

export interface NatsConnectionLike {
  publish(subject: string, data?: Uint8Array, options?: { reply?: string; headers?: MsgHdrs }): void;
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

  get protocolName(): string {
    return NATS_TRANSPORT_PROTOCOL_NAME;
  }

  get protocolVersion(): string {
    return A2A_PROTOCOL_VERSION;
  }

  async getExtendedAgentCard(params: GetExtendedAgentCardRequest, options?: RequestOptions): Promise<AgentCard> {
    return this.sendUnaryRequest('getExtendedAgentCard', params, options);
  }

  async sendMessage(params: SendMessageRequest, options?: RequestOptions): Promise<SendMessageResult> {
    const response = await this.sendUnaryRequest<SendMessageRequest, SendMessageResponse>('sendMessage', params, options);
    return resultFromSendMessageResponse(response);
  }

  async *sendMessageStream(
    params: SendMessageRequest,
    options?: RequestOptions
  ): AsyncGenerator<StreamResponse, void, undefined> {
    yield* this.sendStreamingRequest('sendMessageStream', params, options);
  }

  async createTaskPushNotificationConfig(
    params: TaskPushNotificationConfig,
    options?: RequestOptions
  ): Promise<TaskPushNotificationConfig> {
    return this.sendUnaryRequest('createTaskPushNotificationConfig', params, options);
  }

  async getTaskPushNotificationConfig(
    params: GetTaskPushNotificationConfigRequest,
    options?: RequestOptions
  ): Promise<TaskPushNotificationConfig> {
    return this.sendUnaryRequest('getTaskPushNotificationConfig', params, options);
  }

  async listTaskPushNotificationConfig(
    params: ListTaskPushNotificationConfigsRequest,
    options?: RequestOptions
  ): Promise<ListTaskPushNotificationConfigsResponse> {
    return this.sendUnaryRequest('listTaskPushNotificationConfig', params, options);
  }

  async deleteTaskPushNotificationConfig(
    params: DeleteTaskPushNotificationConfigRequest,
    options?: RequestOptions
  ): Promise<void> {
    await this.sendUnaryRequest('deleteTaskPushNotificationConfig', params, options);
  }

  async getTask(params: GetTaskRequest, options?: RequestOptions): Promise<Task> {
    return this.sendUnaryRequest('getTask', params, options);
  }

  async cancelTask(params: CancelTaskRequest, options?: RequestOptions): Promise<Task> {
    return this.sendUnaryRequest('cancelTask', params, options);
  }

  async listTasks(params: ListTasksRequest, options?: RequestOptions): Promise<ListTasksResponse> {
    return this.sendUnaryRequest('listTasks', params, options);
  }

  async *resubscribeTask(
    params: SubscribeToTaskRequest,
    options?: RequestOptions
  ): AsyncGenerator<StreamResponse, void, undefined> {
    yield* this.sendStreamingRequest('resubscribeTask', params, options);
  }

  private async sendUnaryRequest<TRequest, TResponse>(
    method: A2ANatsTransportMethod,
    params: TRequest,
    options?: RequestOptions
  ): Promise<TResponse> {
    const requestId = this.nextRequestId();
    const message = await this.options.connection.request(
      this.options.subject,
      encodeRequestFrame(method, params, undefined, requestId),
      {
        timeout: this.requestTimeoutMs,
        signal: options?.signal,
        headers: headersFromServiceParameters(options?.serviceParameters),
      }
    );
    const frame = decodeUnaryFrame(message.data);
    this.assertRequestId(frame.requestId, requestId, method);
    if (frame.kind === 'error') {
      throw framePayloadToError(frame.error);
    }
    return decodeResponsePayload<TResponse>(method, frame.payload);
  }

  private async *sendStreamingRequest<TRequest>(
    method: A2ANatsTransportMethod,
    params: TRequest,
    options?: RequestOptions
  ): AsyncGenerator<StreamResponse, void, undefined> {
    const requestId = this.nextRequestId();
    const reply = this.createInbox();
    const subscription = this.options.connection.subscribe(reply) as Subscription | (AsyncIterable<NatsMsgLike> & { unsubscribe(): void });
    const abort = () => subscription.unsubscribe();

    try {
      options?.signal?.addEventListener('abort', abort, { once: true });
      await this.options.connection.flush?.();
      this.options.connection.publish(
        this.options.subject,
        encodeRequestFrame(method, params, undefined, requestId),
        {
          reply,
          headers: headersFromServiceParameters(options?.serviceParameters),
        }
      );

      for await (const message of subscription) {
        const frame = decodeStreamFrame(message.data);
        this.assertRequestId(frame.requestId, requestId, method);
        if (frame.kind === 'complete') {
          return;
        }
        if (frame.kind === 'transport-error') {
          throw new Error(frame.message);
        }
        if (frame.kind === 'error') {
          throw framePayloadToError(frame.error);
        }
        yield StreamResponse.fromJSON(frame.payload);
      }
    } finally {
      options?.signal?.removeEventListener('abort', abort);
      subscription.unsubscribe();
    }
  }

  private nextRequestId(): string {
    return String(this.requestIdCounter++);
  }

  private assertRequestId(
    actual: string | undefined,
    expected: string,
    method: A2ANatsTransportMethod
  ): void {
    if (actual !== expected) {
      throw new Error(`A2A NATS response ID mismatch for ${method}. Expected ${expected}, got ${actual ?? '<missing>'}.`);
    }
  }
}

export function headersFromServiceParameters(serviceParameters: Record<string, string> | undefined): MsgHdrs {
  const result = headers();
  for (const [key, value] of Object.entries(serviceParameters ?? {})) {
    result.set(key, value);
  }
  result.set(A2A_VERSION_HEADER, A2A_PROTOCOL_VERSION);
  return result;
}

export function serviceParametersFromHeaders(messageHeaders: MsgHdrs | undefined): Record<string, string> {
  const result: Record<string, string> = {};
  for (const [key, values] of messageHeaders ?? []) {
    const last = values.at(-1);
    if (last !== undefined) {
      result[key] = last;
    }
  }
  return result;
}
