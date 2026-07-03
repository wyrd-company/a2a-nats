import {
  ContentTypeNotSupportedError,
  ExtendedAgentCardNotConfiguredError,
  InvalidAgentResponseError,
  PushNotificationNotSupportedError,
  RequestMalformedError,
  TaskNotCancelableError,
  TaskNotFoundError,
  UnsupportedOperationError,
  VersionNotSupportedError,
} from '@a2a-js/sdk/client';

import type { A2ANatsErrorFramePayload, A2ANatsErrorInfo } from '../../../protocol/src/index.js';

const errorInfoType = 'type.googleapis.com/google.rpc.ErrorInfo';
const errorDomain = 'a2a-protocol.org';

const errorReasons = {
  ContentTypeNotSupportedError: 'CONTENT_TYPE_NOT_SUPPORTED',
  ExtendedAgentCardNotConfiguredError: 'EXTENDED_AGENT_CARD_NOT_CONFIGURED',
  InvalidAgentResponseError: 'INVALID_AGENT_RESPONSE',
  PushNotificationNotSupportedError: 'PUSH_NOTIFICATION_NOT_SUPPORTED',
  RequestMalformedError: 'INVALID_PARAMS',
  TaskNotCancelableError: 'TASK_NOT_CANCELABLE',
  TaskNotFoundError: 'TASK_NOT_FOUND',
  UnsupportedOperationError: 'UNSUPPORTED_OPERATION',
  VersionNotSupportedError: 'VERSION_NOT_SUPPORTED',
} as const;

const errorsByName = {
  ContentTypeNotSupportedError,
  ExtendedAgentCardNotConfiguredError,
  InvalidAgentResponseError,
  PushNotificationNotSupportedError,
  RequestMalformedError,
  TaskNotCancelableError,
  TaskNotFoundError,
  UnsupportedOperationError,
  VersionNotSupportedError,
} as const;

const errorsByReason = Object.fromEntries(
  Object.entries(errorReasons).map(([name, reason]) => [reason, errorsByName[name as keyof typeof errorsByName]])
);

export function errorToFramePayload(error: unknown): A2ANatsErrorFramePayload {
  const name = error instanceof Error ? error.name : 'Error';
  const message = error instanceof Error ? error.message : String(error);
  const reason = errorReasons[name as keyof typeof errorReasons] ?? 'INTERNAL_ERROR';
  const details: A2ANatsErrorInfo[] = [
    {
      '@type': errorInfoType,
      reason,
      domain: errorDomain,
    },
  ];
  return {
    name,
    message,
    details,
  };
}

export function framePayloadToError(payload: A2ANatsErrorFramePayload): Error {
  const detail = payload.details?.find(isErrorInfo);
  const ErrorClass =
    (payload.name in errorsByName ? errorsByName[payload.name as keyof typeof errorsByName] : undefined) ??
    (detail ? errorsByReason[detail.reason] : undefined);

  return ErrorClass ? new ErrorClass(payload.message) : new A2ANatsProtocolError(payload);
}

export class A2ANatsProtocolError extends Error {
  constructor(public readonly payload: A2ANatsErrorFramePayload) {
    super(`A2A NATS error: ${payload.message}`);
    this.name = 'A2ANatsProtocolError';
  }
}

function isErrorInfo(value: unknown): value is A2ANatsErrorInfo {
  return (
    typeof value === 'object' &&
    value !== null &&
    (value as A2ANatsErrorInfo)['@type'] === errorInfoType &&
    typeof (value as A2ANatsErrorInfo).reason === 'string'
  );
}
