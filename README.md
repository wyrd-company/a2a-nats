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
- transport protocol name: `NATS`

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
the Node matrix. A separate integration job starts `nats:2-alpine` and runs the
`NATS_URL`-gated real NATS test once on Node 22.

Publishing runs on SemVer git tags without a `v` prefix:

- `1.2.3`
- `1.2.3-alpha.1`

The publish workflow verifies that the tag exactly matches `package.json`
`version`, then publishes to npmjs.org and GitHub Package Registry. npmjs.org
publishing requires `NPM_TOKEN`; GitHub Packages uses `GITHUB_TOKEN`.
