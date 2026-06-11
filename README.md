# a2a-nats

Agent2Agent (A2A) custom protocol binding for NATS.io.

This repository is structured for multiple A2A language SDK bindings. The shared
NATS wire contract lives in `protocol/`; SDK-specific implementations live under
`sdks/<language>/`.

## Packages

The initial package is the TypeScript binding:

- package: `@wyrd-company/a2a-nats`
- A2A SDK: `@a2a-js/sdk`
- NATS client: `@nats-io/transport-node`
- core transport protocol name: `NATS`
- durable JetStream transport protocol name: `NATS+JS`

## Layout

```text
protocol/
  src/                 Shared subject naming and NATS frame contracts.
sdks/
  typescript/src/      A2A JS client transport factory and server listener.
test/                  TypeScript binding tests with an in-memory NATS broker.
```

Future SDKs should reuse the protocol frame shapes and subject conventions rather
than inventing language-specific wire formats.

## Wire Contract

Unary calls publish an `a2a-nats/1` request frame to an agent subject and receive
one JSON-RPC response on the NATS reply subject.

Streaming calls publish the same request frame with a generated reply inbox. The
server publishes stream frames to that inbox:

- `response`: contains one A2A JSON-RPC response event.
- `complete`: marks normal stream completion.
- `transport-error`: reports a transport-level stream failure.

Agent subjects can be generated with `a2aNatsAgentSubject()`:

```ts
import { a2aNatsAgentSubject } from '@wyrd-company/a2a-nats';

const subject = a2aNatsAgentSubject({ namespace: 'a2a', agentId: 'agent-a' });
// a2a.agent.agent-a.rpc
```

Agent cards should advertise NATS with `preferredTransport` or
`additionalInterfaces`:

```ts
const agentCard = {
  // ...
  url: 'nats://a2a.agent.agent-a.rpc',
  preferredTransport: 'NATS',
};
```

## AgentCard Registry

For NATS-native discovery, publish AgentCards to a dedicated JetStream KV bucket.
The bucket is intentionally not part of the key; operators choose the bucket as
the registry boundary. Keys use this shape:

```text
<namespace>.agents.<agentId>
```

Use `namespace` as the collision boundary for an A2A server group, deployment,
tenant, environment, or other scope where `agentId` values are unique. A single
bucket can therefore hold cards for multiple A2A server groups without key
collisions.

```ts
import { JetStreamKvAgentCardRegistry } from '@wyrd-company/a2a-nats';

const registry = new JetStreamKvAgentCardRegistry({
  connection: nc,
  bucket: 'A2A_AGENT_CARDS',
  namespace: 'server-a',
});

await registry.publish({ agentId: 'agent-a', card: agentCard });

const entry = await registry.resolve('agent-a');
const cardsInNamespace = await registry.list();
```

This registry requires JetStream. The core NATS transport only requires core
NATS request/reply.

## Durable JetStream Transport

The core `NATS` transport uses request/reply and is intentionally ephemeral.
For durable enterprise message traffic, use the `NATS+JS` transport. It persists
A2A requests and responses through JetStream streams:

- request stream: stores frames on subjects such as
  `<namespace>.agent.<agentId>.requests`
- response stream: stores frames on subjects such as
  `<namespace>.client.<clientId>.responses`
- server durable consumer: pulls requests from the request stream
- client request consumers: pull matching persisted response frames

```ts
import {
  JetStreamA2AClientTransport,
  JetStreamA2AServer,
  a2aJetStreamRequestSubject,
} from '@wyrd-company/a2a-nats';

const namespace = 'server-a';
const requestSubject = a2aJetStreamRequestSubject({
  namespace,
  agentId: 'agent-a',
});

const server = new JetStreamA2AServer({
  connection: nc,
  requestSubject,
  requestStream: 'A2A_REQUESTS',
  responseStream: 'A2A_RESPONSES',
  responseSubjects: [`${namespace}.client.*.responses`],
  requestHandler,
});

const client = new JetStreamA2AClientTransport({
  connection: nc,
  namespace,
  clientId: 'client-a',
  requestSubject,
  requestStream: 'A2A_REQUESTS',
  responseStream: 'A2A_RESPONSES',
});

await server.ready();
await client.ready();
```

Agent cards can advertise this as a separate interface:

```ts
const agentCard = {
  // ...
  additionalInterfaces: [
    {
      transport: 'NATS+JS',
      url: 'nats+js://server-a/agent/agent-a/requests',
    },
  ],
};
```

JetStream stream creation is automatic by default. Set `createStreams: false`
when streams are provisioned by operations.

## TypeScript Client

```ts
import { ClientFactory, ClientFactoryOptions } from '@a2a-js/sdk/client';
import { connect } from '@nats-io/transport-node';
import { NatsTransportFactory } from '@wyrd-company/a2a-nats';

const nc = await connect({ servers: 'nats://localhost:4222' });

const factory = new ClientFactory(
  ClientFactoryOptions.createFrom(ClientFactoryOptions.default, {
    transports: [new NatsTransportFactory({ connection: nc })],
    preferredTransports: ['NATS'],
  })
);

const client = await factory.createFromAgentCard(agentCard);
const response = await client.sendMessage({
  message: {
    kind: 'message',
    role: 'user',
    messageId: crypto.randomUUID(),
    parts: [{ kind: 'text', text: 'hello' }],
  },
});
```

## TypeScript Server

```ts
import { DefaultRequestHandler, InMemoryTaskStore } from '@a2a-js/sdk/server';
import { connect } from '@nats-io/transport-node';
import { NatsA2AServer, a2aNatsAgentSubject } from '@wyrd-company/a2a-nats';

const nc = await connect({ servers: 'nats://localhost:4222' });
const subject = a2aNatsAgentSubject({ agentId: 'agent-a' });

const requestHandler = new DefaultRequestHandler(
  agentCard,
  new InMemoryTaskStore(),
  agentExecutor
);

const server = new NatsA2AServer({
  connection: nc,
  subject,
  requestHandler,
});

await server.ready();
```

## Development

```bash
npm install
npm run verify
```

`npm run verify` runs typecheck, tests, and build.

## Release

CI runs on pushes and pull requests to `main`. The default verification job runs
the Node matrix. A separate integration job starts `nats:2-alpine` with JetStream
enabled and runs the `NATS_URL`-gated real NATS test once on Node 22.

Publishing runs on SemVer git tags without a `v` prefix:

- `1.2.3`
- `1.2.3-alpha.1`

The publish workflow verifies that the tag exactly matches `package.json`
`version`, then publishes to npmjs.org and GitHub Package Registry. npmjs.org
publishing requires `NPM_TOKEN`; GitHub Packages uses `GITHUB_TOKEN`.
