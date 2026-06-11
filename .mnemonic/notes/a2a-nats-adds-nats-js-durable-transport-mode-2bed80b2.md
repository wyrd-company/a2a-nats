---
title: A2A NATS adds NATS+JS durable transport mode
tags:
  - a2a
  - nats
  - jetstream
  - typescript
  - protocol
lifecycle: permanent
createdAt: '2026-06-11T02:00:18.338Z'
updatedAt: '2026-06-11T02:00:18.338Z'
role: decision
alwaysLoad: false
project: github-com-wyrd-company-a2a-nats
projectName: a2a-nats
memoryVersion: 1
---
A2A NATS now has two message transport modes.

`NATS` remains the core request/reply transport for low-latency ephemeral A2A traffic. `NATS+JS` is a separate durable JetStream transport mode instead of changing `NATS` semantics.

The `NATS+JS` TypeScript transport persists request frames to a request stream subject such as `<namespace>.agent.<agentId>.requests` and persists response/event frames to response stream subjects such as `<namespace>.client.<clientId>.responses`. Servers use a durable pull consumer over the request stream and acknowledge requests after response frames are published. Clients create request-scoped durable response consumers and skip persisted frames for other request IDs, allowing old responses to coexist in the stream.

The shared wire request frame has an optional `responseSubject` field used by JetStream mode. Core NATS request/reply ignores it and continues to use the NATS reply subject.
