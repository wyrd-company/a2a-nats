import type { JSONRPCRequest, JSONRPCResponse } from '@a2a-js/sdk';

import { A2A_NATS_PROTOCOL } from './subjects.js';

export interface A2ANatsRequestFrame {
  readonly protocol: typeof A2A_NATS_PROTOCOL;
  readonly request: JSONRPCRequest;
  readonly serviceParameters?: Record<string, string>;
}

export interface A2ANatsStreamResponseFrame {
  readonly protocol: typeof A2A_NATS_PROTOCOL;
  readonly kind: 'response';
  readonly response: JSONRPCResponse;
}

export interface A2ANatsStreamCompleteFrame {
  readonly protocol: typeof A2A_NATS_PROTOCOL;
  readonly kind: 'complete';
  readonly id: JSONRPCRequest['id'];
}

export interface A2ANatsStreamTransportErrorFrame {
  readonly protocol: typeof A2A_NATS_PROTOCOL;
  readonly kind: 'transport-error';
  readonly id: JSONRPCRequest['id'];
  readonly message: string;
}

export type A2ANatsStreamFrame =
  | A2ANatsStreamResponseFrame
  | A2ANatsStreamCompleteFrame
  | A2ANatsStreamTransportErrorFrame;

