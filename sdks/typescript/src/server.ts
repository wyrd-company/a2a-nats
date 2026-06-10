import { HTTP_EXTENSION_HEADER, Extensions, type JSONRPCResponse } from '@a2a-js/sdk';
import { A2AError, JsonRpcTransportHandler, ServerCallContext, type A2ARequestHandler } from '@a2a-js/sdk/server';

import { A2A_NATS_PROTOCOL, type A2ANatsStreamFrame } from '../../../protocol/src/index.js';
import { decodeRequestFrame, encodeJson } from './codec.js';
import type { NatsConnectionLike, NatsMsgLike } from './transport.js';

export interface NatsA2AServerOptions {
  readonly connection: NatsConnectionLike;
  readonly subject: string;
  readonly requestHandler: A2ARequestHandler;
  readonly queue?: string;
}

export class NatsA2AServer {
  private readonly transportHandler: JsonRpcTransportHandler;
  private readonly subscription: AsyncIterable<NatsMsgLike> & { unsubscribe(): void };
  private readonly readyPromise: Promise<void>;
  private closed = false;

  constructor(private readonly options: NatsA2AServerOptions) {
    this.transportHandler = new JsonRpcTransportHandler(options.requestHandler);
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
    let requestId: string | number | null = null;
    try {
      const frame = decodeRequestFrame(message.data);
      requestId = frame.request.id ?? null;
      const response = await this.transportHandler.handle(
        frame.request,
        this.createContext(frame.serviceParameters)
      );

      if (isAsyncIterable(response)) {
        await this.publishStream(message, requestId, response);
      } else {
        this.respondUnary(message, response);
      }
    } catch (error) {
      const response = this.errorResponse(requestId, error);
      if (message.reply) {
        this.options.connection.publish(message.reply, encodeJson(response));
      } else {
        message.respond?.(encodeJson(response));
      }
    }
  }

  private respondUnary(message: NatsMsgLike, response: JSONRPCResponse): void {
    if (message.respond?.(encodeJson(response))) {
      return;
    }
    if (message.reply) {
      this.options.connection.publish(message.reply, encodeJson(response));
    }
  }

  private async publishStream(
    message: NatsMsgLike,
    requestId: string | number | null,
    responseStream: AsyncGenerator<JSONRPCResponse, void, undefined>
  ): Promise<void> {
    if (!message.reply) {
      this.respondUnary(message, this.errorResponse(requestId, new Error('Streaming requests require a NATS reply subject.')));
      return;
    }

    try {
      for await (const response of responseStream) {
        const frame: A2ANatsStreamFrame = {
          protocol: A2A_NATS_PROTOCOL,
          kind: 'response',
          response,
        };
        this.options.connection.publish(message.reply, encodeJson(frame));
      }
      this.options.connection.publish(
        message.reply,
        encodeJson({
          protocol: A2A_NATS_PROTOCOL,
          kind: 'complete',
          id: requestId,
        } satisfies A2ANatsStreamFrame)
      );
    } catch (error) {
      this.options.connection.publish(
        message.reply,
        encodeJson({
          protocol: A2A_NATS_PROTOCOL,
          kind: 'transport-error',
          id: requestId,
          message: error instanceof Error ? error.message : 'A2A NATS stream failed',
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
        : A2AError.internalError(error instanceof Error ? error.message : 'A2A NATS transport error');
    return {
      jsonrpc: '2.0',
      id,
      error: a2aError.toJSONRPCError(),
    };
  }
}

export function serveA2AOverNats(options: NatsA2AServerOptions): NatsA2AServer {
  return new NatsA2AServer(options);
}

function isAsyncIterable<T>(value: unknown): value is AsyncGenerator<T, void, undefined> {
  return typeof value === 'object' && value !== null && Symbol.asyncIterator in value;
}

