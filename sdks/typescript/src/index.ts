export {
  NatsA2AClientTransport,
  NatsTransportFactory,
  createNatsTransportFactory,
} from './transport.js';
export type { NatsConnectionLike, NatsMsgLike, NatsRequestOptions, NatsTransportFactoryOptions } from './transport.js';

export { NatsA2AServer, serveA2AOverNats } from './server.js';
export type { NatsA2AServerOptions } from './server.js';

export {
  A2A_NATS_PROTOCOL,
  DEFAULT_A2A_NATS_NAMESPACE,
  NATS_TRANSPORT_PROTOCOL_NAME,
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

