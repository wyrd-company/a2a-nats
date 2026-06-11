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
import { HTTP_EXTENSION_HEADER, Extensions } from '@a2a-js/sdk';
import { A2AError, JsonRpcTransportHandler, ServerCallContext, type A2ARequestHandler } from '@a2a-js/sdk/server';
import type { RequestOptions, Transport, TransportFactory } from '@a2a-js/sdk/client';
import {
  AckPolicy,
  DeliverPolicy,
  jetstream,
  jetstreamManager,
  type Consumer,
  type JsMsg,
} from '@nats-io/jetstream';
import type { NatsConnection } from '@nats-io/transport-node';

import {
  A2A_NATS_PROTOCOL,
  DEFAULT_A2A_NATS_NAMESPACE,
  NATS_JETSTREAM_TRANSPORT_PROTOCOL_NAME,
  a2aJetStreamResponseSubject,
  natsSubjectFromAgentUrl,
  sanitizeSubjectToken,
  type A2ANatsStreamFrame,
} from '../../../protocol/src/index.js';
import { decodeJson, decodeRequestFrame, decodeStreamFrame, encodeJson, encodeRequestFrame } from './codec.js';
import { mapJsonRpcError } from './errors.js';

type A2AStreamEventData = Message | Task | TaskStatusUpdateEvent | TaskArtifactUpdateEvent;
type SendMessageResult = Message | Task;

export interface JetStreamA2ATransportFactoryOptions {
  readonly connection: NatsConnection;
  readonly clientId: string;
  readonly namespace?: string;
  readonly requestStream?: string;
  readonly responseStream?: string;
  readonly requestTimeoutMs?: number;
  readonly createStreams?: boolean;
}

export interface JetStreamA2ATransportOptions extends JetStreamA2ATransportFactoryOptions {
  readonly requestSubject: string;
}

export interface JetStreamA2AServerOptions {
  readonly connection: NatsConnection;
  readonly requestHandler: A2ARequestHandler;
  readonly requestSubject: string;
  readonly requestStream?: string;
  readonly responseStream?: string;
  readonly responseSubjects?: string[];
  readonly durableName?: string;
  readonly createStreams?: boolean;
  readonly pullExpiresMs?: number;
}

const defaultRequestStream = 'A2A_REQUESTS';
const defaultResponseStream = 'A2A_RESPONSES';
const defaultRequestTimeoutMs = 30_000;
const defaultPullExpiresMs = 1_000;

export class JetStreamA2ATransportFactory implements TransportFactory {
  constructor(private readonly options: JetStreamA2ATransportFactoryOptions) {}

  get protocolName(): string {
    return NATS_JETSTREAM_TRANSPORT_PROTOCOL_NAME;
  }

  async create(url: string, _agentCard: AgentCard): Promise<Transport> {
    const transport = new JetStreamA2AClientTransport({
      ...this.options,
      requestSubject: natsSubjectFromAgentUrl(url),
    });
    await transport.ready();
    return transport;
  }
}

export function createJetStreamA2ATransportFactory(
  options: JetStreamA2ATransportFactoryOptions
): JetStreamA2ATransportFactory {
  return new JetStreamA2ATransportFactory(options);
}

export class JetStreamA2AClientTransport implements Transport {
  private requestIdCounter = 1;
  private readonly requestStream: string;
  private readonly responseStream: string;
  private readonly responseSubject: string;
  private readonly requestTimeoutMs: number;
  private readonly readyPromise: Promise<void>;

  constructor(private readonly options: JetStreamA2ATransportOptions) {
    this.requestStream = options.requestStream ?? defaultRequestStream;
    this.responseStream = options.responseStream ?? defaultResponseStream;
    this.responseSubject = a2aJetStreamResponseSubject({
      namespace: options.namespace,
      clientId: options.clientId,
    });
    this.requestTimeoutMs = options.requestTimeoutMs ?? defaultRequestTimeoutMs;
    this.readyPromise = this.initialize();
  }

  async ready(): Promise<void> {
    await this.readyPromise;
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
    yield* this.sendStreamingRequest('message/stream', params, options);
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
      JSONRPCResponse & { result: TaskPushNotificationConfig[] }
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
    yield* this.sendStreamingRequest('tasks/resubscribe', params, options);
  }

  private async initialize(): Promise<void> {
    if (this.options.createStreams === false) {
      return;
    }
    await ensureStream(this.options.connection, this.requestStream, [this.options.requestSubject]);
    await ensureStream(this.options.connection, this.responseStream, [this.responseSubject]);
  }

  private async sendRpcRequest<TParams, TResponse extends JSONRPCResponse>(
    method: string,
    params: TParams,
    options?: RequestOptions
  ): Promise<TResponse> {
    const request = this.createRequest(method, params);
    const consumer = await this.createResponseConsumer(request.id);
    try {
      await this.publishRequest(request, options);
      const response = await this.nextResponse(consumer, request.id, method);
      if ('error' in response) {
        throw mapJsonRpcError(response);
      }
      return response as TResponse;
    } finally {
      await consumer.delete();
    }
  }

  private async *sendStreamingRequest<TParams>(
    method: string,
    params: TParams,
    options?: RequestOptions
  ): AsyncGenerator<A2AStreamEventData, void, undefined> {
    const request = this.createRequest(method, params);
    const consumer = await this.createResponseConsumer(request.id);
    try {
      await this.publishRequest(request, options);
      while (true) {
        const frame = await this.nextStreamFrame(consumer, request.id, method);
        if (frame.kind === 'complete') {
          return;
        }
        if (frame.kind === 'transport-error') {
          throw new Error(frame.message);
        }

        const response = frame.response;
        this.assertResponseId(response, request.id, method);
        if ('error' in response) {
          throw mapJsonRpcError(response);
        }
        if (!('result' in response)) {
          throw new Error(`JetStream response for ${method} did not contain a result.`);
        }
        yield response.result as A2AStreamEventData;
      }
    } finally {
      await consumer.delete();
    }
  }

  private async publishRequest(request: JSONRPCRequest, options?: RequestOptions): Promise<void> {
    await this.readyPromise;
    const js = jetstream(this.options.connection);
    await js.publish(
      this.options.requestSubject,
      encodeRequestFrame(request, options?.serviceParameters, this.responseSubject)
    );
  }

  private async createResponseConsumer(requestId: JSONRPCRequest['id']): Promise<Consumer> {
    await this.readyPromise;
    const jsm = await jetstreamManager(this.options.connection);
    const durable = durableName(`${this.options.clientId}_${String(requestId)}_responses`);
    await addOrUpdateConsumer(jsm, this.responseStream, durable, this.responseSubject);
    return jetstream(this.options.connection).consumers.get(this.responseStream, durable);
  }

  private async nextResponse(
    consumer: Consumer,
    requestId: JSONRPCRequest['id'],
    method: string
  ): Promise<JSONRPCResponse> {
    while (true) {
      const message = await nextMessage(consumer, this.requestTimeoutMs);
      const frame = decodeStreamFrame(message.data);
      message.ack();
      if (frame.kind !== 'response') {
        continue;
      }
      if (frame.response.id !== requestId) {
        continue;
      }
      return frame.response;
    }
  }

  private async nextStreamFrame(
    consumer: Consumer,
    requestId: JSONRPCRequest['id'],
    method: string
  ): Promise<A2ANatsStreamFrame> {
    while (true) {
      const message = await nextMessage(consumer, this.requestTimeoutMs);
      const frame = decodeStreamFrame(message.data);
      message.ack();
      if ('id' in frame && frame.id !== requestId) {
        continue;
      }
      if (frame.kind === 'response') {
        if (frame.response.id !== requestId) {
          continue;
        }
      }
      return frame;
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

  private assertResponseId(response: JSONRPCResponse, requestId: JSONRPCRequest['id'], method: string): void {
    if (response.id !== requestId) {
      throw new Error(`JSON-RPC response ID mismatch for ${method}. Expected ${requestId}, got ${response.id}.`);
    }
  }
}

export class JetStreamA2AServer {
  private readonly transportHandler: JsonRpcTransportHandler;
  private readonly requestStream: string;
  private readonly responseStream: string;
  private readonly durableName: string;
  private readonly pullExpiresMs: number;
  private readonly readyPromise: Promise<Consumer>;
  private readonly servePromise: Promise<void>;
  private closed = false;

  constructor(private readonly options: JetStreamA2AServerOptions) {
    this.transportHandler = new JsonRpcTransportHandler(options.requestHandler);
    this.requestStream = options.requestStream ?? defaultRequestStream;
    this.responseStream = options.responseStream ?? defaultResponseStream;
    this.durableName = durableName(options.durableName ?? `${options.requestSubject}_server`);
    this.pullExpiresMs = options.pullExpiresMs ?? defaultPullExpiresMs;
    this.readyPromise = this.initialize();
    this.servePromise = this.serveLoop();
  }

  async ready(): Promise<void> {
    await this.readyPromise;
  }

  async close(): Promise<void> {
    this.closed = true;
    await this.servePromise;
  }

  private async initialize(): Promise<Consumer> {
    if (this.options.createStreams !== false) {
      await ensureStream(this.options.connection, this.requestStream, [this.options.requestSubject]);
      await ensureStream(
        this.options.connection,
        this.responseStream,
        this.options.responseSubjects ?? [`${DEFAULT_A2A_NATS_NAMESPACE}.client.*.responses`]
      );
    }
    const jsm = await jetstreamManager(this.options.connection);
    await addOrUpdateConsumer(jsm, this.requestStream, this.durableName, this.options.requestSubject);
    return jetstream(this.options.connection).consumers.get(this.requestStream, this.durableName);
  }

  private async serveLoop(): Promise<void> {
    const consumer = await this.readyPromise;
    while (!this.closed) {
      const message = await consumer.next({ expires: this.pullExpiresMs });
      if (!message) {
        continue;
      }
      await this.handleMessage(message);
    }
  }

  private async handleMessage(message: JsMsg): Promise<void> {
    let requestId: string | number | null = null;
    let responseSubject: string | undefined;
    try {
      const frame = decodeRequestFrame(message.data);
      requestId = frame.request.id ?? null;
      responseSubject = frame.responseSubject;
      if (!responseSubject) {
        throw new Error('JetStream A2A request frame did not include responseSubject.');
      }

      const response = await this.transportHandler.handle(
        frame.request,
        this.createContext(frame.serviceParameters)
      );

      if (isAsyncIterable(response)) {
        await this.publishStream(responseSubject, requestId, response);
      } else {
        await this.publishResponse(responseSubject, response);
      }
      message.ack();
    } catch (error) {
      if (responseSubject) {
        await this.publishResponse(responseSubject, this.errorResponse(requestId, error));
        message.ack();
      } else {
        message.term(error instanceof Error ? error.message : 'Invalid A2A JetStream request');
      }
    }
  }

  private async publishResponse(responseSubject: string, response: JSONRPCResponse): Promise<void> {
    const js = jetstream(this.options.connection);
    await js.publish(
      responseSubject,
      encodeJson({
        protocol: A2A_NATS_PROTOCOL,
        kind: 'response',
        response,
      } satisfies A2ANatsStreamFrame)
    );
  }

  private async publishStream(
    responseSubject: string,
    requestId: string | number | null,
    responseStream: AsyncGenerator<JSONRPCResponse, void, undefined>
  ): Promise<void> {
    const js = jetstream(this.options.connection);
    try {
      for await (const response of responseStream) {
        await js.publish(
          responseSubject,
          encodeJson({
            protocol: A2A_NATS_PROTOCOL,
            kind: 'response',
            response,
          } satisfies A2ANatsStreamFrame)
        );
      }
      await js.publish(
        responseSubject,
        encodeJson({
          protocol: A2A_NATS_PROTOCOL,
          kind: 'complete',
          id: requestId,
        } satisfies A2ANatsStreamFrame)
      );
    } catch (error) {
      await js.publish(
        responseSubject,
        encodeJson({
          protocol: A2A_NATS_PROTOCOL,
          kind: 'transport-error',
          id: requestId,
          message: error instanceof Error ? error.message : 'A2A JetStream stream failed',
        } satisfies A2ANatsStreamFrame)
      );
    }
  }

  private createContext(serviceParameters: Record<string, string> | undefined): ServerCallContext | undefined {
    const extensionParameter = serviceParameters?.[HTTP_EXTENSION_HEADER];
    return extensionParameter ? new ServerCallContext(Extensions.parseServiceParameter(extensionParameter)) : undefined;
  }

  private errorResponse(id: string | number | null, error: unknown): JSONRPCResponse {
    const a2aError =
      error instanceof A2AError
        ? error
        : A2AError.internalError(error instanceof Error ? error.message : 'A2A JetStream transport error');
    return {
      jsonrpc: '2.0',
      id,
      error: a2aError.toJSONRPCError(),
    };
  }
}

export function serveA2AOverJetStream(options: JetStreamA2AServerOptions): JetStreamA2AServer {
  return new JetStreamA2AServer(options);
}

async function ensureStream(
  connection: NatsConnection,
  name: string,
  subjects: string[]
): Promise<void> {
  const jsm = await jetstreamManager(connection);
  try {
    await jsm.streams.info(name);
    return;
  } catch {
    try {
      await jsm.streams.add({ name, subjects });
    } catch (error) {
      if (error instanceof Error && error.message.includes('stream name already in use')) {
        return;
      }
      throw error;
    }
  }
}

async function addOrUpdateConsumer(
  jsm: Awaited<ReturnType<typeof jetstreamManager>>,
  stream: string,
  durable: string,
  filterSubject: string
): Promise<void> {
  const config = {
    durable_name: durable,
    ack_policy: AckPolicy.Explicit,
    deliver_policy: DeliverPolicy.All,
    filter_subject: filterSubject,
  };
  try {
    await jsm.consumers.add(stream, config);
  } catch {
    await jsm.consumers.update(stream, durable, config);
  }
}

async function nextMessage(consumer: Consumer, timeout: number): Promise<JsMsg> {
  const message = await consumer.next({ expires: timeout });
  if (!message) {
    throw new Error(`Timed out waiting for JetStream A2A response after ${timeout}ms.`);
  }
  return message;
}

function durableName(value: string): string {
  return sanitizeSubjectToken(value).slice(0, 128);
}

function isAsyncIterable<T>(value: unknown): value is AsyncGenerator<T, void, undefined> {
  return typeof value === 'object' && value !== null && Symbol.asyncIterator in value;
}
