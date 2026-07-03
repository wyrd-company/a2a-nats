import {
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
  Message,
  SendMessageRequest,
  SendMessageResponse,
  StreamResponse,
  SubscribeToTaskRequest,
  Task,
  TaskPushNotificationConfig,
  type SendMessageResult,
} from '@a2a-js/sdk';

import {
  A2A_NATS_PROTOCOL,
  type A2ANatsRequestFrame,
  type A2ANatsStreamFrame,
  type A2ANatsTransportMethod,
  type A2ANatsUnaryFrame,
} from '../../../protocol/src/index.js';

const encoder = new TextEncoder();
const decoder = new TextDecoder();

interface MessageFns<T> {
  fromJSON(object: unknown): T;
  toJSON(message: T): unknown;
}

export interface Empty {
  readonly [key: string]: never;
}

export const EmptyResponse: MessageFns<Empty> = {
  fromJSON: () => ({}),
  toJSON: () => ({}),
};

export interface MethodCodec<TRequest, TResponse> {
  readonly request: MessageFns<TRequest>;
  readonly response: MessageFns<TResponse>;
  readonly streaming?: boolean;
}

export const methodCodecs = {
  getExtendedAgentCard: {
    request: GetExtendedAgentCardRequest,
    response: AgentCard,
  },
  sendMessage: {
    request: SendMessageRequest,
    response: SendMessageResponse,
  },
  sendMessageStream: {
    request: SendMessageRequest,
    response: StreamResponse,
    streaming: true,
  },
  createTaskPushNotificationConfig: {
    request: TaskPushNotificationConfig,
    response: TaskPushNotificationConfig,
  },
  getTaskPushNotificationConfig: {
    request: GetTaskPushNotificationConfigRequest,
    response: TaskPushNotificationConfig,
  },
  listTaskPushNotificationConfig: {
    request: ListTaskPushNotificationConfigsRequest,
    response: ListTaskPushNotificationConfigsResponse,
  },
  deleteTaskPushNotificationConfig: {
    request: DeleteTaskPushNotificationConfigRequest,
    response: EmptyResponse,
  },
  getTask: {
    request: GetTaskRequest,
    response: Task,
  },
  cancelTask: {
    request: CancelTaskRequest,
    response: Task,
  },
  listTasks: {
    request: ListTasksRequest,
    response: ListTasksResponse,
  },
  resubscribeTask: {
    request: SubscribeToTaskRequest,
    response: StreamResponse,
    streaming: true,
  },
} satisfies Record<A2ANatsTransportMethod, MethodCodec<unknown, unknown>>;

export function encodeJson(value: unknown): Uint8Array {
  return encoder.encode(JSON.stringify(value));
}

export function decodeJson(data: Uint8Array): unknown {
  return JSON.parse(decoder.decode(data));
}

export function encodeRequestFrame<TRequest>(
  method: A2ANatsTransportMethod,
  request: TRequest,
  responseSubject?: string,
  requestId?: string
): Uint8Array {
  const codec = methodCodecs[method] as MethodCodec<TRequest, unknown>;
  const frame: A2ANatsRequestFrame = {
    protocol: A2A_NATS_PROTOCOL,
    method,
    payload: codec.request.toJSON(request),
    responseSubject,
    requestId,
  };
  return encodeJson(frame);
}

export function decodeRequestFrame(data: Uint8Array): A2ANatsRequestFrame {
  const decoded = decodeJson(data);
  if (!isRequestFrame(decoded)) {
    throw new Error('NATS message did not contain an A2A request frame');
  }
  return decoded;
}

export function decodeRequestPayload<TRequest>(frame: A2ANatsRequestFrame): TRequest {
  const codec = methodCodecs[frame.method] as MethodCodec<TRequest, unknown>;
  return codec.request.fromJSON(frame.payload);
}

export function encodeUnaryResponseFrame<TResponse>(
  method: A2ANatsTransportMethod,
  response: TResponse,
  requestId?: string
): Uint8Array {
  const codec = methodCodecs[method] as MethodCodec<unknown, TResponse>;
  const frame: A2ANatsUnaryFrame = {
    protocol: A2A_NATS_PROTOCOL,
    kind: 'response',
    requestId,
    payload: codec.response.toJSON(response),
  };
  return encodeJson(frame);
}

export function decodeUnaryFrame(data: Uint8Array): A2ANatsUnaryFrame {
  const decoded = decodeJson(data);
  if (!isUnaryFrame(decoded)) {
    throw new Error('NATS message did not contain an A2A unary response frame');
  }
  return decoded;
}

export function decodeResponsePayload<TResponse>(
  method: A2ANatsTransportMethod,
  payload: unknown
): TResponse {
  const codec = methodCodecs[method] as MethodCodec<unknown, TResponse>;
  return codec.response.fromJSON(payload);
}

export function encodeStreamResponseFrame(
  streamResponse: StreamResponse,
  requestId?: string
): Uint8Array {
  const frame: A2ANatsStreamFrame = {
    protocol: A2A_NATS_PROTOCOL,
    kind: 'response',
    requestId,
    payload: StreamResponse.toJSON(streamResponse),
  };
  return encodeJson(frame);
}

export function decodeStreamFrame(data: Uint8Array): A2ANatsStreamFrame {
  const decoded = decodeJson(data);
  if (!isObject(decoded) || decoded.protocol !== A2A_NATS_PROTOCOL || typeof decoded.kind !== 'string') {
    throw new Error('NATS message did not contain an A2A stream frame');
  }
  return decoded as unknown as A2ANatsStreamFrame;
}

export function sendMessageResponseFromResult(result: SendMessageResult): SendMessageResponse {
  if (isTask(result)) {
    return { payload: { $case: 'task', value: result } };
  }
  return { payload: { $case: 'message', value: result } };
}

export function resultFromSendMessageResponse(response: SendMessageResponse): SendMessageResult {
  if (!response.payload) {
    throw new Error('A2A NATS SendMessageResponse did not contain a payload.');
  }
  return response.payload.value;
}

function isRequestFrame(value: unknown): value is A2ANatsRequestFrame {
  return (
    isObject(value) &&
    value.protocol === A2A_NATS_PROTOCOL &&
    typeof value.method === 'string' &&
    value.method in methodCodecs &&
    'payload' in value
  );
}

function isUnaryFrame(value: unknown): value is A2ANatsUnaryFrame {
  return (
    isObject(value) &&
    value.protocol === A2A_NATS_PROTOCOL &&
    (value.kind === 'response' || value.kind === 'error')
  );
}

function isTask(value: SendMessageResult): value is Task {
  return 'status' in value;
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}
