export {
  NatsA2AClientTransport,
  NatsTransportFactory,
  createNatsTransportFactory,
} from './transport.js';
export type { NatsConnectionLike, NatsMsgLike, NatsRequestOptions, NatsTransportFactoryOptions } from './transport.js';

export { NatsA2AServer, serveA2AOverNats } from './server.js';
export type { NatsA2AServerOptions } from './server.js';

export {
  JetStreamA2AClientTransport,
  JetStreamA2AServer,
  JetStreamA2ATransportFactory,
  createJetStreamA2ATransportFactory,
  serveA2AOverJetStream,
} from './jetstream.js';
export type {
  JetStreamA2AServerOptions,
  JetStreamA2ATransportFactoryOptions,
  JetStreamA2ATransportOptions,
} from './jetstream.js';

export {
  JetStreamKvAgentCardRegistry,
  createJetStreamKvAgentCardRegistry,
} from './registry.js';
export type {
  AgentCardRegistryEntry,
  AgentCardRegistryOptions,
  PublishAgentCardOptions,
} from './registry.js';

export {
  A2A_NATS_TRANSPORT_EXTENSION_URI,
  a2aNatsTransportExtension,
  type A2aNatsTransportExtensionDeclaration,
  type A2aNatsTransportExtensionOptions,
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
} from '../../../protocol/src/index.js';

export type {
  A2ANatsRequestFrame,
  A2ANatsStreamCompleteFrame,
  A2ANatsStreamFrame,
  A2ANatsStreamResponseFrame,
  A2ANatsStreamTransportErrorFrame,
} from '../../../protocol/src/index.js';
