import { A2A_NATS_PROTOCOL } from './subjects.js';

export type A2ANatsTransportMethod =
  | 'getExtendedAgentCard'
  | 'sendMessage'
  | 'sendMessageStream'
  | 'createTaskPushNotificationConfig'
  | 'getTaskPushNotificationConfig'
  | 'listTaskPushNotificationConfig'
  | 'deleteTaskPushNotificationConfig'
  | 'getTask'
  | 'cancelTask'
  | 'listTasks'
  | 'resubscribeTask';

export interface A2ANatsRequestFrame {
  readonly protocol: typeof A2A_NATS_PROTOCOL;
  readonly method: A2ANatsTransportMethod;
  readonly payload: unknown;
  readonly requestId?: string;
  readonly responseSubject?: string;
}

export interface A2ANatsErrorInfo {
  readonly '@type': 'type.googleapis.com/google.rpc.ErrorInfo';
  readonly reason: string;
  readonly domain: 'a2a-protocol.org';
  readonly metadata?: Record<string, string>;
}

export interface A2ANatsErrorFramePayload {
  readonly name: string;
  readonly message: string;
  readonly details?: A2ANatsErrorInfo[];
}

export interface A2ANatsUnaryErrorFrame {
  readonly protocol: typeof A2A_NATS_PROTOCOL;
  readonly kind: 'error';
  readonly requestId?: string;
  readonly error: A2ANatsErrorFramePayload;
}

export interface A2ANatsStreamResponseFrame {
  readonly protocol: typeof A2A_NATS_PROTOCOL;
  readonly kind: 'response';
  readonly requestId?: string;
  readonly payload: unknown;
}

export interface A2ANatsStreamErrorFrame {
  readonly protocol: typeof A2A_NATS_PROTOCOL;
  readonly kind: 'error';
  readonly requestId?: string;
  readonly error: A2ANatsErrorFramePayload;
}

export interface A2ANatsStreamCompleteFrame {
  readonly protocol: typeof A2A_NATS_PROTOCOL;
  readonly kind: 'complete';
  readonly requestId?: string;
}

export interface A2ANatsStreamTransportErrorFrame {
  readonly protocol: typeof A2A_NATS_PROTOCOL;
  readonly kind: 'transport-error';
  readonly requestId?: string;
  readonly message: string;
}

export type A2ANatsStreamFrame =
  | A2ANatsStreamResponseFrame
  | A2ANatsStreamErrorFrame
  | A2ANatsStreamCompleteFrame
  | A2ANatsStreamTransportErrorFrame;

export type A2ANatsUnaryFrame = A2ANatsStreamResponseFrame | A2ANatsUnaryErrorFrame;
