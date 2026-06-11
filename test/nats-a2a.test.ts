import assert from 'node:assert/strict';
import { test } from 'node:test';

import type {
  AgentCard,
  Message,
  MessageSendParams,
  Task,
  TaskIdParams,
  TaskPushNotificationConfig,
  TaskQueryParams,
} from '@a2a-js/sdk';
import type { A2ARequestHandler } from '@a2a-js/sdk/server';

import {
  NATS_TRANSPORT_PROTOCOL_NAME,
  NatsA2AClientTransport,
  NatsA2AServer,
  a2aNatsAgentCardKey,
  a2aNatsAgentCardNamespaceFilter,
  a2aNatsAgentSubject,
  natsSubjectFromAgentUrl,
} from '../sdks/typescript/src/index.js';
import { FakeNatsBroker } from './fake-nats.js';

test('routes unary A2A JSON-RPC calls over NATS', async () => {
  const broker = new FakeNatsBroker();
  const subject = a2aNatsAgentSubject({ namespace: 'wyrd.a2a', agentId: 'agent-a' });
  const handler = new StubRequestHandler();
  const server = new NatsA2AServer({ connection: broker, subject, requestHandler: handler });
  const client = new NatsA2AClientTransport({
    connection: broker,
    subject,
    createInbox: () => broker.createInbox(),
  });

  await server.ready();

  const result = await client.sendMessage(sendParams('hello'));

  assert.equal(subject, 'wyrd_a2a.agent.agent-a.rpc');
  assert.equal(result.kind, 'message');
  assert.equal(result.parts[0]?.kind, 'text');
  assert.equal(result.parts[0]?.kind === 'text' ? result.parts[0].text : '', 'echo: hello');

  server.close();
});

test('routes streaming A2A events over a NATS reply inbox', async () => {
  const broker = new FakeNatsBroker();
  const subject = a2aNatsAgentSubject({ agentId: 'streaming-agent' });
  const server = new NatsA2AServer({
    connection: broker,
    subject,
    requestHandler: new StubRequestHandler(),
  });
  const client = new NatsA2AClientTransport({
    connection: broker,
    subject,
    createInbox: () => broker.createInbox(),
  });

  await server.ready();

  const events = [];
  for await (const event of client.sendMessageStream(sendParams('stream'))) {
    events.push(event);
  }

  assert.deepEqual(
    events.map(event => event.kind),
    ['task', 'status-update']
  );

  server.close();
});

test('parses NATS agent URLs and exposes the NATS protocol name', () => {
  assert.equal(NATS_TRANSPORT_PROTOCOL_NAME, 'NATS');
  assert.equal(natsSubjectFromAgentUrl('nats://a2a.agent.agent-a.rpc'), 'a2a.agent.agent-a.rpc');
  assert.equal(natsSubjectFromAgentUrl('nats://a2a/agent/agent-a/rpc'), 'a2a.agent.agent-a.rpc');
  assert.equal(natsSubjectFromAgentUrl('a2a.agent.agent-a.rpc'), 'a2a.agent.agent-a.rpc');
  assert.equal(
    a2aNatsAgentCardKey({ namespace: 'server-a', agentId: 'agent-a' }),
    'server-a.agents.agent-a'
  );
  assert.equal(a2aNatsAgentCardNamespaceFilter('server-a'), 'server-a.agents.*');
});

class StubRequestHandler implements A2ARequestHandler {
  private readonly agentCard: AgentCard = {
    name: 'Stub Agent',
    description: 'A test agent',
    version: '0.1.0',
    protocolVersion: '0.3.0',
    url: 'nats://a2a.agent.stub.rpc',
    preferredTransport: 'NATS',
    capabilities: {
      streaming: true,
      pushNotifications: true,
    },
    defaultInputModes: ['text/plain'],
    defaultOutputModes: ['text/plain'],
    skills: [],
  };

  async getAgentCard(): Promise<AgentCard> {
    return this.agentCard;
  }

  async getAuthenticatedExtendedAgentCard(): Promise<AgentCard> {
    return this.agentCard;
  }

  async sendMessage(params: MessageSendParams): Promise<Message> {
    return agentMessage(`echo: ${textFrom(params)}`);
  }

  async *sendMessageStream(): AsyncGenerator<Task, void, undefined> {
    yield {
      kind: 'task',
      id: 'task-1',
      contextId: 'context-1',
      status: { state: 'working' },
    };
    yield {
      kind: 'status-update',
      taskId: 'task-1',
      contextId: 'context-1',
      status: { state: 'completed' },
      final: true,
    } as never;
  }

  async getTask(params: TaskQueryParams): Promise<Task> {
    return {
      kind: 'task',
      id: params.id,
      contextId: 'context-1',
      status: { state: 'completed' },
    };
  }

  async cancelTask(params: TaskIdParams): Promise<Task> {
    return {
      kind: 'task',
      id: params.id,
      contextId: 'context-1',
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
        id: 'config-1',
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
      id: 'task-1',
      contextId: 'context-1',
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
