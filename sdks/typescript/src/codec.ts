import type { JSONRPCRequest } from '@a2a-js/sdk';
import { A2A_NATS_PROTOCOL, type A2ANatsRequestFrame, type A2ANatsStreamFrame } from '../../../protocol/src/index.js';

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export function encodeJson(value: unknown): Uint8Array {
  return encoder.encode(JSON.stringify(value));
}

export function decodeJson(data: Uint8Array): unknown {
  return JSON.parse(decoder.decode(data));
}

export function encodeRequestFrame(
  request: JSONRPCRequest,
  serviceParameters?: Record<string, string>,
  responseSubject?: string
): Uint8Array {
  const frame: A2ANatsRequestFrame = {
    protocol: A2A_NATS_PROTOCOL,
    request,
    serviceParameters,
    responseSubject,
  };
  return encodeJson(frame);
}

export function decodeRequestFrame(data: Uint8Array): A2ANatsRequestFrame {
  const decoded = decodeJson(data);
  if (isRequestFrame(decoded)) {
    return decoded;
  }
  if (isJsonRpcRequest(decoded)) {
    return {
      protocol: A2A_NATS_PROTOCOL,
      request: decoded,
    };
  }
  throw new Error('NATS message did not contain an A2A request frame');
}

export function decodeStreamFrame(data: Uint8Array): A2ANatsStreamFrame {
  const decoded = decodeJson(data);
  if (!isObject(decoded) || decoded.protocol !== A2A_NATS_PROTOCOL || typeof decoded.kind !== 'string') {
    throw new Error('NATS message did not contain an A2A stream frame');
  }
  return decoded as unknown as A2ANatsStreamFrame;
}

function isRequestFrame(value: unknown): value is A2ANatsRequestFrame {
  return isObject(value) && value.protocol === A2A_NATS_PROTOCOL && isJsonRpcRequest(value.request);
}

function isJsonRpcRequest(value: unknown): value is JSONRPCRequest {
  return isObject(value) && value.jsonrpc === '2.0' && typeof value.method === 'string';
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}
