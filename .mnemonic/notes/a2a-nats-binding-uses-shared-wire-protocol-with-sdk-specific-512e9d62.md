---
title: A2A NATS binding uses shared wire protocol with SDK-specific adapters
tags:
  - a2a
  - nats
  - typescript
  - protocol
  - packaging
lifecycle: permanent
createdAt: '2026-06-10T14:24:44.768Z'
updatedAt: '2026-06-10T14:24:44.768Z'
role: decision
alwaysLoad: false
project: github-com-wyrd-company-a2a-nats
projectName: a2a-nats
memoryVersion: 1
---
A2A NATS binding uses a shared `protocol/` contract and SDK-specific adapters under `sdks/<language>/`.

The initial TypeScript adapter for `@a2a-js/sdk` implements A2A transport name `NATS`, uses `@nats-io/transport-node`, and maps A2A JSON-RPC calls onto NATS subjects. Unary requests receive one JSON-RPC response. Streaming requests use a generated reply inbox and explicit `a2a-nats/1` stream frames: `response`, `complete`, and `transport-error`.

The package publishes `@wyrd-company/a2a-nats` from built `dist/` artifacts and keeps tests out of the tarball via a separate `tsconfig.build.json`. Release automation publishes to npmjs.org and GitHub Package Registry only from bare SemVer tags matching `package.json` version, with no `v` prefix.
