import type { JSONRPCErrorResponse } from '@a2a-js/sdk';
import {
  AuthenticatedExtendedCardNotConfiguredError,
  ContentTypeNotSupportedError,
  InvalidAgentResponseError,
  PushNotificationNotSupportedError,
  TaskNotCancelableError,
  TaskNotFoundError,
  UnsupportedOperationError,
} from '@a2a-js/sdk/client';

export function mapJsonRpcError(response: JSONRPCErrorResponse): Error {
  switch (response.error.code) {
    case -32001:
      return new TaskNotFoundError(response.error.message);
    case -32002:
      return new TaskNotCancelableError(response.error.message);
    case -32003:
      return new PushNotificationNotSupportedError(response.error.message);
    case -32004:
      return new UnsupportedOperationError(response.error.message);
    case -32005:
      return new ContentTypeNotSupportedError(response.error.message);
    case -32006:
      return new InvalidAgentResponseError(response.error.message);
    case -32007:
      return new AuthenticatedExtendedCardNotConfiguredError(response.error.message);
    default:
      return new A2ANatsJsonRpcError(response);
  }
}

export class A2ANatsJsonRpcError extends Error {
  constructor(public readonly response: JSONRPCErrorResponse) {
    super(
      `JSON-RPC error: ${response.error.message} (Code: ${response.error.code}) Data: ${JSON.stringify(response.error.data ?? {})}`
    );
    this.name = 'A2ANatsJsonRpcError';
  }
}

