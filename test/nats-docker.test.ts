import assert from 'node:assert/strict';
import { test } from 'node:test';

import type {
  AgentCard,
  Message,
  MessageSendParams,
  Task,
  TaskArtifactUpdateEvent,
  TaskIdParams,
  TaskPushNotificationConfig,
  TaskQueryParams,
  TaskStatusUpdateEvent,
} from '@a2a-js/sdk';
import type { A2ARequestHandler } from '@a2a-js/sdk/server';
import { connect } from '@nats-io/transport-node';

import {
  NatsA2AClientTransport,
  NatsA2AServer,
  JetStreamA2AClientTransport,
  JetStreamA2AServer,
  JetStreamKvAgentCardRegistry,
  a2aJetStreamRequestSubject,
  a2aNatsAgentSubject,
} from '../sdks/typescript/src/index.js';

const natsUrl = process.env.NATS_URL;

test('routes A2A calls through a real NATS server', { skip: !natsUrl }, async () => {
  assert.ok(natsUrl);

  const subject = a2aNatsAgentSubject({
    namespace: 'integration.a2a',
    agentId: `agent-${Date.now()}`,
  });
  const connection = await connect({ servers: natsUrl });
  const server = new NatsA2AServer({
    connection,
    subject,
    requestHandler: new IntegrationRequestHandler(),
  });
  const client = new NatsA2AClientTransport({
    connection,
    subject,
    requestTimeoutMs: 2_000,
  });

  try {
    await server.ready();

    const unary = await client.sendMessage(sendParams('real nats'));
    assert.equal(unary.kind, 'message');
    assert.equal(unary.parts[0]?.kind, 'text');
    assert.equal(unary.parts[0]?.kind === 'text' ? unary.parts[0].text : '', 'echo: real nats');

    const streamEvents = [];
    for await (const event of client.sendMessageStream(sendParams('stream'))) {
      streamEvents.push(event.kind);
    }
    assert.deepEqual(streamEvents, ['task', 'status-update']);
  } finally {
    await server.close();
    await connection.drain();
  }
});

test('routes A2A calls through JetStream-backed durable subjects', { skip: !natsUrl }, async () => {
  assert.ok(natsUrl);

  const connection = await connect({ servers: natsUrl });
  const suffix = Date.now();
  const requestSubject = a2aJetStreamRequestSubject({
    namespace: `integration-js-${suffix}`,
    agentId: 'agent-a',
  });
  const responseSubjectPattern = `integration-js-${suffix}.client.*.responses`;
  const requestStream = `A2A_REQUESTS_${suffix}`;
  const responseStream = `A2A_RESPONSES_${suffix}`;
  const server = new JetStreamA2AServer({
    connection,
    requestSubject,
    requestStream,
    responseStream,
    responseSubjects: [responseSubjectPattern],
    requestHandler: new IntegrationRequestHandler(),
  });
  const client = new JetStreamA2AClientTransport({
    connection,
    requestSubject,
    requestStream,
    responseStream,
    namespace: `integration-js-${suffix}`,
    clientId: 'client-a',
    requestTimeoutMs: 2_000,
  });

  try {
    await server.ready();
    await client.ready();

    const unary = await client.sendMessage(sendParams('durable nats'));
    assert.equal(unary.kind, 'message');
    assert.equal(unary.parts[0]?.kind, 'text');
    assert.equal(unary.parts[0]?.kind === 'text' ? unary.parts[0].text : '', 'echo: durable nats');

    const streamEvents = [];
    for await (const event of client.sendMessageStream(sendParams('stream'))) {
      streamEvents.push(event.kind);
    }
    assert.deepEqual(streamEvents, ['task', 'status-update']);
  } finally {
    server.close();
    await connection.drain();
  }
});

test('publishes AgentCards to a dedicated JetStream KV bucket', { skip: !natsUrl }, async () => {
  assert.ok(natsUrl);

  const connection = await connect({ servers: natsUrl });
  const registry = new JetStreamKvAgentCardRegistry({
    connection,
    bucket: `A2A_AGENT_CARDS_${Date.now()}`,
    namespace: 'server-a',
  });

  try {
    const card = new IntegrationRequestHandler().card();
    const published = await registry.publish({ agentId: 'agent-a', card });
    const resolved = await registry.resolve('agent-a');
    const listed = await registry.list();

    assert.equal(published.key, 'server-a.agents.agent-a');
    assert.equal(resolved?.card.name, card.name);
    assert.deepEqual(listed.map(entry => entry.key), ['server-a.agents.agent-a']);
  } finally {
    await connection.drain();
  }
});

class IntegrationRequestHandler implements A2ARequestHandler {
  private readonly agentCard: AgentCard = {
    name: 'Integration Agent',
    description: 'A test agent backed by a real NATS server',
    version: '0.1.0',
    protocolVersion: '0.3.0',
    url: 'nats://integration.a2a.agent.integration.rpc',
    preferredTransport: 'NATS',
    capabilities: {
      streaming: true,
      pushNotifications: true,
    },
    defaultInputModes: ['text/plain'],
    defaultOutputModes: ['text/plain'],
    skills: [],
  };

  card(): AgentCard {
    return this.agentCard;
  }

  async getAgentCard(): Promise<AgentCard> {
    return this.agentCard;
  }

  async getAuthenticatedExtendedAgentCard(): Promise<AgentCard> {
    return this.agentCard;
  }

  async sendMessage(params: MessageSendParams): Promise<Message> {
    return agentMessage(`echo: ${textFrom(params)}`);
  }

  async *sendMessageStream(): AsyncGenerator<Task | TaskStatusUpdateEvent | TaskArtifactUpdateEvent, void, undefined> {
    yield {
      kind: 'task',
      id: 'task-real',
      contextId: 'context-real',
      status: { state: 'working' },
    };
    yield {
      kind: 'status-update',
      taskId: 'task-real',
      contextId: 'context-real',
      status: { state: 'completed' },
      final: true,
    } as never;
  }

  async getTask(params: TaskQueryParams): Promise<Task> {
    return {
      kind: 'task',
      id: params.id,
      contextId: 'context-real',
      status: { state: 'completed' },
    };
  }

  async cancelTask(params: TaskIdParams): Promise<Task> {
    return {
      kind: 'task',
      id: params.id,
      contextId: 'context-real',
      status: { state: 'canceled' },
    };
  }

  async setTaskPushNotificationConfig(params: TaskPushNotificationConfig): Promise<TaskPushNotificationConfig> {
    return params;
  }

  async getTaskPushNotificationConfig(params: TaskIdParams): Promise<TaskPushNotificationConfig> {
    return {
      taskId: params.id,
      pushNotificationConfig: {
        id: 'config-real',
        url: 'https://example.test/a2a',
      },
    };
  }

  async listTaskPushNotificationConfigs(params: TaskIdParams): Promise<TaskPushNotificationConfig[]> {
    return [await this.getTaskPushNotificationConfig(params)];
  }

  async deleteTaskPushNotificationConfig(): Promise<void> {}

  async *resubscribe(): AsyncGenerator<Task, void, undefined> {
    yield {
      kind: 'task',
      id: 'task-real',
      contextId: 'context-real',
      status: { state: 'completed' },
    };
  }
}

function sendParams(text: string): MessageSendParams {
  return {
    message: {
      kind: 'message',
      role: 'user',
      messageId: `message-${text}`,
      parts: [{ kind: 'text', text }],
    },
  };
}

function agentMessage(text: string): Message {
  return {
    kind: 'message',
    role: 'agent',
    messageId: `agent-${text}`,
    parts: [{ kind: 'text', text }],
  };
}

function textFrom(params: MessageSendParams): string {
  const part = params.message.parts[0];
  return part?.kind === 'text' ? part.text : '';
}
