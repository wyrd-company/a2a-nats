import {
  A2A_PROTOCOL_VERSION,
  A2A_VERSION_HEADER,
  HTTP_EXTENSION_HEADER,
  Extensions,
  type SendMessageResponse,
} from '@a2a-js/sdk';
import { ServerCallContext, type A2ARequestHandler } from '@a2a-js/sdk/server';

import { A2A_NATS_PROTOCOL, type A2ANatsStreamFrame } from '../../../protocol/src/index.js';
import {
  decodeRequestFrame,
  decodeRequestPayload,
  encodeJson,
  encodeStreamResponseFrame,
  encodeUnaryResponseFrame,
  sendMessageResponseFromResult,
} from './codec.js';
import { errorToFramePayload } from './errors.js';
import {
  serviceParametersFromHeaders,
  type NatsConnectionLike,
  type NatsMsgLike,
} from './transport.js';

export interface NatsA2AServerOptions {
  readonly connection: NatsConnectionLike;
  readonly subject: string;
  readonly requestHandler: A2ARequestHandler;
  readonly queue?: string;
}

export class NatsA2AServer {
  private readonly subscription: AsyncIterable<NatsMsgLike> & { unsubscribe(): void };
  private readonly readyPromise: Promise<void>;
  private closed = false;

  constructor(private readonly options: NatsA2AServerOptions) {
    this.subscription = options.connection.subscribe(options.subject, { queue: options.queue });
    this.readyPromise = options.connection.flush?.() ?? Promise.resolve();
    void this.serveLoop();
  }

  async ready(): Promise<void> {
    await this.readyPromise;
  }

  close(): void {
    if (this.closed) {
      return;
    }
    this.closed = true;
    this.subscription.unsubscribe();
  }

  private async serveLoop(): Promise<void> {
    await this.readyPromise;
    try {
      for await (const message of this.subscription) {
        void this.handleMessage(message);
      }
    } finally {
      this.close();
    }
  }

  private async handleMessage(message: NatsMsgLike): Promise<void> {
    let requestId: string | undefined;
    try {
      const frame = decodeRequestFrame(message.data);
      requestId = frame.requestId;
      const context = createContext(serviceParametersFromHeaders(message.headers));

      switch (frame.method) {
        case 'getExtendedAgentCard':
          this.respondUnary(
            message,
            encodeUnaryResponseFrame(
              frame.method,
              await this.options.requestHandler.getAuthenticatedExtendedAgentCard(
                decodeRequestPayload(frame),
                context
              ),
              requestId
            )
          );
          return;
        case 'sendMessage': {
          const result = await this.options.requestHandler.sendMessage(decodeRequestPayload(frame), context);
          this.respondUnary(
            message,
            encodeUnaryResponseFrame<SendMessageResponse>(
              frame.method,
              sendMessageResponseFromResult(result),
              requestId
            )
          );
          return;
        }
        case 'sendMessageStream':
          await this.publishStream(
            message,
            requestId,
            this.options.requestHandler.sendMessageStream(decodeRequestPayload(frame), context)
          );
          return;
        case 'createTaskPushNotificationConfig':
          this.respondUnary(
            message,
            encodeUnaryResponseFrame(
              frame.method,
              await this.options.requestHandler.createTaskPushNotificationConfig(
                decodeRequestPayload(frame),
                context
              ),
              requestId
            )
          );
          return;
        case 'getTaskPushNotificationConfig':
          this.respondUnary(
            message,
            encodeUnaryResponseFrame(
              frame.method,
              await this.options.requestHandler.getTaskPushNotificationConfig(
                decodeRequestPayload(frame),
                context
              ),
              requestId
            )
          );
          return;
        case 'listTaskPushNotificationConfig':
          this.respondUnary(
            message,
            encodeUnaryResponseFrame(
              frame.method,
              await this.options.requestHandler.listTaskPushNotificationConfigs(
                decodeRequestPayload(frame),
                context
              ),
              requestId
            )
          );
          return;
        case 'deleteTaskPushNotificationConfig':
          await this.options.requestHandler.deleteTaskPushNotificationConfig(
            decodeRequestPayload(frame),
            context
          );
          this.respondUnary(message, encodeUnaryResponseFrame(frame.method, {}, requestId));
          return;
        case 'getTask':
          this.respondUnary(
            message,
            encodeUnaryResponseFrame(
              frame.method,
              await this.options.requestHandler.getTask(decodeRequestPayload(frame), context),
              requestId
            )
          );
          return;
        case 'cancelTask':
          this.respondUnary(
            message,
            encodeUnaryResponseFrame(
              frame.method,
              await this.options.requestHandler.cancelTask(decodeRequestPayload(frame), context),
              requestId
            )
          );
          return;
        case 'listTasks':
          this.respondUnary(
            message,
            encodeUnaryResponseFrame(
              frame.method,
              await this.options.requestHandler.listTasks(decodeRequestPayload(frame), context),
              requestId
            )
          );
          return;
        case 'resubscribeTask':
          await this.publishStream(
            message,
            requestId,
            this.options.requestHandler.resubscribe(decodeRequestPayload(frame), context)
          );
          return;
      }
    } catch (error) {
      this.respondUnary(message, encodeJson(errorFrame(requestId, error)));
    }
  }

  private respondUnary(message: NatsMsgLike, response: Uint8Array): void {
    if (message.respond?.(response)) {
      return;
    }
    if (message.reply) {
      this.options.connection.publish(message.reply, response);
    }
  }

  private async publishStream(
    message: NatsMsgLike,
    requestId: string | undefined,
    responseStream: AsyncGenerator<import('@a2a-js/sdk').StreamResponse, void, undefined>
  ): Promise<void> {
    if (!message.reply) {
      this.respondUnary(message, encodeJson(errorFrame(requestId, new Error('Streaming requests require a NATS reply subject.'))));
      return;
    }

    try {
      for await (const response of responseStream) {
        this.options.connection.publish(message.reply, encodeStreamResponseFrame(response, requestId));
      }
      this.options.connection.publish(
        message.reply,
        encodeJson({
          protocol: A2A_NATS_PROTOCOL,
          kind: 'complete',
          requestId,
        } satisfies A2ANatsStreamFrame)
      );
    } catch (error) {
      this.options.connection.publish(message.reply, encodeJson(errorFrame(requestId, error)));
    }
  }
}

export function serveA2AOverNats(options: NatsA2AServerOptions): NatsA2AServer {
  return new NatsA2AServer(options);
}

export function createContext(serviceParameters: Record<string, string>): ServerCallContext {
  return new ServerCallContext({
    requestedExtensions: Extensions.parseServiceParameter(serviceParameters[HTTP_EXTENSION_HEADER]),
    requestedVersion: serviceParameters[A2A_VERSION_HEADER] ?? A2A_PROTOCOL_VERSION,
  });
}

export function errorFrame(requestId: string | undefined, error: unknown): A2ANatsStreamFrame {
  return {
    protocol: A2A_NATS_PROTOCOL,
    kind: 'error',
    requestId,
    error: errorToFramePayload(error),
  };
}
