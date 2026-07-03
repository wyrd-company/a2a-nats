export {
  A2A_NATS_TRANSPORT_EXTENSION_URI,
  a2aNatsTransportExtension,
} from './extensions.js';

export type {
  A2aNatsTransportExtensionDeclaration,
  A2aNatsTransportExtensionOptions,
} from './extensions.js';

export {
  A2A_NATS_PROTOCOL,
  DEFAULT_A2A_NATS_NAMESPACE,
  NATS_JETSTREAM_TRANSPORT_PROTOCOL_NAME,
  NATS_TRANSPORT_PROTOCOL_NAME,
  a2aJetStreamRequestSubject,
  a2aJetStreamResponseSubject,
  a2aNatsAgentCardKey,
  a2aNatsAgentCardNamespaceFilter,
  a2aNatsAgentSubject,
  natsSubjectFromAgentUrl,
  sanitizeSubjectToken,
} from './subjects.js';

export type {
  A2ANatsRequestFrame,
  A2ANatsErrorFramePayload,
  A2ANatsErrorInfo,
  A2ANatsStreamCompleteFrame,
  A2ANatsStreamErrorFrame,
  A2ANatsStreamFrame,
  A2ANatsStreamResponseFrame,
  A2ANatsStreamTransportErrorFrame,
  A2ANatsTransportMethod,
  A2ANatsUnaryErrorFrame,
  A2ANatsUnaryFrame,
} from './wire.js';
