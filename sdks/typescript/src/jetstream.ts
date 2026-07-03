import {
  A2A_PROTOCOL_VERSION,
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
  SendMessageRequest,
  type SendMessageResponse,
  StreamResponse,
  SubscribeToTaskRequest,
  Task,
  TaskPushNotificationConfig,
  type SendMessageResult,
} from '@a2a-js/sdk';
import type { A2ARequestHandler } from '@a2a-js/sdk/server';
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
  type A2ANatsTransportMethod,
} from '../../../protocol/src/index.js';
import {
  decodeRequestFrame,
  decodeRequestPayload,
  decodeResponsePayload,
  decodeStreamFrame,
  encodeJson,
  encodeRequestFrame,
  encodeStreamResponseFrame,
  encodeUnaryResponseFrame,
  resultFromSendMessageResponse,
  sendMessageResponseFromResult,
} from './codec.js';
import { framePayloadToError } from './errors.js';
import { createContext, errorFrame } from './server.js';
import { headersFromServiceParameters, serviceParametersFromHeaders } from './transport.js';

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

  get protocolName(): string {
    return NATS_JETSTREAM_TRANSPORT_PROTOCOL_NAME;
  }

  get protocolVersion(): string {
    return A2A_PROTOCOL_VERSION;
  }

  async ready(): Promise<void> {
    await this.readyPromise;
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

  private async initialize(): Promise<void> {
    if (this.options.createStreams === false) {
      return;
    }
    await ensureStream(this.options.connection, this.requestStream, [this.options.requestSubject]);
    await ensureStream(this.options.connection, this.responseStream, [this.responseSubject]);
  }

  private async sendUnaryRequest<TRequest, TResponse>(
    method: A2ANatsTransportMethod,
    params: TRequest,
    options?: RequestOptions
  ): Promise<TResponse> {
    const requestId = this.nextRequestId();
    const consumer = await this.createResponseConsumer(requestId);
    try {
      await this.publishRequest(method, params, requestId, options);
      const frame = await this.nextResponseFrame(consumer, requestId, method);
      if (frame.kind === 'error') {
        throw framePayloadToError(frame.error);
      }
      if (frame.kind !== 'response') {
        throw new Error(`JetStream response for ${method} was not a response frame.`);
      }
      return decodeResponsePayload<TResponse>(method, frame.payload);
    } finally {
      await consumer.delete();
    }
  }

  private async *sendStreamingRequest<TRequest>(
    method: A2ANatsTransportMethod,
    params: TRequest,
    options?: RequestOptions
  ): AsyncGenerator<StreamResponse, void, undefined> {
    const requestId = this.nextRequestId();
    const consumer = await this.createResponseConsumer(requestId);
    try {
      await this.publishRequest(method, params, requestId, options);
      while (true) {
        const frame = await this.nextResponseFrame(consumer, requestId, method);
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
      await consumer.delete();
    }
  }

  private async publishRequest<TRequest>(
    method: A2ANatsTransportMethod,
    params: TRequest,
    requestId: string,
    options?: RequestOptions
  ): Promise<void> {
    await this.readyPromise;
    const js = jetstream(this.options.connection);
    await js.publish(
      this.options.requestSubject,
      encodeRequestFrame(method, params, this.responseSubject, requestId),
      { headers: headersFromServiceParameters(options?.serviceParameters) }
    );
  }

  private async createResponseConsumer(requestId: string): Promise<Consumer> {
    await this.readyPromise;
    const jsm = await jetstreamManager(this.options.connection);
    const durable = durableName(`${this.options.clientId}_${requestId}_responses`);
    await addOrUpdateConsumer(jsm, this.responseStream, durable, this.responseSubject);
    return jetstream(this.options.connection).consumers.get(this.responseStream, durable);
  }

  private async nextResponseFrame(
    consumer: Consumer,
    requestId: string,
    method: A2ANatsTransportMethod
  ): Promise<A2ANatsStreamFrame> {
    while (true) {
      const message = await nextMessage(consumer, this.requestTimeoutMs);
      const frame = decodeStreamFrame(message.data);
      message.ack();
      if (frame.requestId !== requestId) {
        continue;
      }
      if (frame.kind === 'transport-error' || frame.kind === 'complete' || frame.kind === 'error' || frame.kind === 'response') {
        return frame;
      }
      throw new Error(`Unexpected JetStream response frame for ${method}.`);
    }
  }

  private nextRequestId(): string {
    return String(this.requestIdCounter++);
  }
}

export class JetStreamA2AServer {
  private readonly requestStream: string;
  private readonly responseStream: string;
  private readonly durableName: string;
  private readonly pullExpiresMs: number;
  private readonly readyPromise: Promise<Consumer>;
  private readonly servePromise: Promise<void>;
  private readonly inFlight = new Set<Promise<void>>();
  private closed = false;

  constructor(private readonly options: JetStreamA2AServerOptions) {
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
    await Promise.allSettled(this.inFlight);
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
      const work = this.handleMessage(message).finally(() => this.inFlight.delete(work));
      this.inFlight.add(work);
    }
  }

  private async handleMessage(message: JsMsg): Promise<void> {
    let requestId: string | undefined;
    let responseSubject: string | undefined;
    try {
      const frame = decodeRequestFrame(message.data);
      requestId = frame.requestId;
      responseSubject = frame.responseSubject;
      if (!responseSubject) {
        throw new Error('JetStream A2A request frame did not include responseSubject.');
      }

      const context = createContext(serviceParametersFromHeaders(message.headers));
      await this.dispatch(frame, context, responseSubject);
      message.ack();
    } catch (error) {
      if (responseSubject) {
        await this.publishResponse(responseSubject, errorFrame(requestId, error));
        message.ack();
      } else {
        message.term(error instanceof Error ? error.message : 'Invalid A2A JetStream request');
      }
    }
  }

  private async dispatch(
    frame: ReturnType<typeof decodeRequestFrame>,
    context: import('@a2a-js/sdk/server').ServerCallContext,
    responseSubject: string
  ): Promise<void> {
    switch (frame.method) {
      case 'getExtendedAgentCard':
        await this.publishResponse(
          responseSubject,
          responseFrame(frame.method, await this.options.requestHandler.getAuthenticatedExtendedAgentCard(decodeRequestPayload(frame), context), frame.requestId)
        );
        return;
      case 'sendMessage': {
        const result = await this.options.requestHandler.sendMessage(decodeRequestPayload(frame), context);
        await this.publishResponse(responseSubject, responseFrame(frame.method, sendMessageResponseFromResult(result), frame.requestId));
        return;
      }
      case 'sendMessageStream':
        await this.publishStream(responseSubject, frame.requestId, this.options.requestHandler.sendMessageStream(decodeRequestPayload(frame), context));
        return;
      case 'createTaskPushNotificationConfig':
        await this.publishResponse(
          responseSubject,
          responseFrame(frame.method, await this.options.requestHandler.createTaskPushNotificationConfig(decodeRequestPayload(frame), context), frame.requestId)
        );
        return;
      case 'getTaskPushNotificationConfig':
        await this.publishResponse(
          responseSubject,
          responseFrame(frame.method, await this.options.requestHandler.getTaskPushNotificationConfig(decodeRequestPayload(frame), context), frame.requestId)
        );
        return;
      case 'listTaskPushNotificationConfig':
        await this.publishResponse(
          responseSubject,
          responseFrame(frame.method, await this.options.requestHandler.listTaskPushNotificationConfigs(decodeRequestPayload(frame), context), frame.requestId)
        );
        return;
      case 'deleteTaskPushNotificationConfig':
        await this.options.requestHandler.deleteTaskPushNotificationConfig(decodeRequestPayload(frame), context);
        await this.publishResponse(responseSubject, responseFrame(frame.method, {}, frame.requestId));
        return;
      case 'getTask':
        await this.publishResponse(responseSubject, responseFrame(frame.method, await this.options.requestHandler.getTask(decodeRequestPayload(frame), context), frame.requestId));
        return;
      case 'cancelTask':
        await this.publishResponse(responseSubject, responseFrame(frame.method, await this.options.requestHandler.cancelTask(decodeRequestPayload(frame), context), frame.requestId));
        return;
      case 'listTasks':
        await this.publishResponse(responseSubject, responseFrame(frame.method, await this.options.requestHandler.listTasks(decodeRequestPayload(frame), context), frame.requestId));
        return;
      case 'resubscribeTask':
        await this.publishStream(responseSubject, frame.requestId, this.options.requestHandler.resubscribe(decodeRequestPayload(frame), context));
        return;
    }
  }

  private async publishResponse(responseSubject: string, frame: A2ANatsStreamFrame): Promise<void> {
    await jetstream(this.options.connection).publish(responseSubject, encodeJson(frame));
  }

  private async publishStream(
    responseSubject: string,
    requestId: string | undefined,
    responseStream: AsyncGenerator<StreamResponse, void, undefined>
  ): Promise<void> {
    const js = jetstream(this.options.connection);
    try {
      for await (const response of responseStream) {
        await js.publish(responseSubject, encodeStreamResponseFrame(response, requestId));
      }
      await js.publish(
        responseSubject,
        encodeJson({
          protocol: A2A_NATS_PROTOCOL,
          kind: 'complete',
          requestId,
        } satisfies A2ANatsStreamFrame)
      );
    } catch (error) {
      await js.publish(responseSubject, encodeJson(errorFrame(requestId, error)));
    }
  }
}

export function serveA2AOverJetStream(options: JetStreamA2AServerOptions): JetStreamA2AServer {
  return new JetStreamA2AServer(options);
}

function responseFrame<TResponse>(
  method: A2ANatsTransportMethod,
  response: TResponse,
  requestId: string | undefined
): Extract<A2ANatsStreamFrame, { kind: 'response' }> {
  return decodeStreamFrame(encodeUnaryResponseFrame(method, response, requestId)) as Extract<A2ANatsStreamFrame, { kind: 'response' }>;
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
