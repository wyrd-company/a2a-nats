import assert from 'node:assert/strict';
import { test } from 'node:test';

import {
  A2A_PROTOCOL_VERSION,
  Message,
  Role,
  SendMessageRequest,
  StreamResponse,
  TaskState,
  type AgentCard,
  type CancelTaskRequest,
  type DeleteTaskPushNotificationConfigRequest,
  type GetExtendedAgentCardRequest,
  type GetTaskPushNotificationConfigRequest,
  type GetTaskRequest,
  type ListTaskPushNotificationConfigsRequest,
  type ListTaskPushNotificationConfigsResponse,
  type ListTasksRequest,
  type ListTasksResponse,
  type SendMessageResult,
  type SubscribeToTaskRequest,
  type Task,
  type TaskPushNotificationConfig,
} from '@a2a-js/sdk';
import type { A2ARequestHandler, ServerCallContext } from '@a2a-js/sdk/server';
import { connect } from '@nats-io/transport-node';

import {
  JetStreamA2AClientTransport,
  JetStreamA2AServer,
  JetStreamKvAgentCardRegistry,
  NatsA2AClientTransport,
  NatsA2AServer,
  a2aJetStreamRequestSubject,
  a2aNatsAgentSubject,
} from '../sdks/typescript/src/index.js';

const natsUrl = process.env.NATS_URL;

test('routes A2A v1 calls through a real NATS server', { skip: !natsUrl }, async () => {
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

    const unary = await client.sendMessage(sendRequest('real nats'));
    assert.equal(messageText(assertMessage(unary)), 'echo: real nats');

    const streamEvents = [];
    for await (const event of client.sendMessageStream(sendRequest('stream'))) {
      streamEvents.push(event.payload?.$case);
    }
    assert.deepEqual(streamEvents, ['task', 'statusUpdate']);
  } finally {
    server.close();
    await connection.drain();
  }
});

test('routes A2A v1 calls through JetStream-backed durable subjects', { skip: !natsUrl }, async () => {
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

    const unary = await client.sendMessage(sendRequest('durable nats'));
    assert.equal(messageText(assertMessage(unary)), 'echo: durable nats');

    const streamEvents = [];
    for await (const event of client.sendMessageStream(sendRequest('stream'))) {
      streamEvents.push(event.payload?.$case);
    }
    assert.deepEqual(streamEvents, ['task', 'statusUpdate']);
  } finally {
    await server.close();
    await connection.drain();
  }
});

test('round-trips v1 AgentCards through a dedicated JetStream KV bucket', { skip: !natsUrl }, async () => {
  assert.ok(natsUrl);

  const connection = await connect({ servers: natsUrl });
  const registry = new JetStreamKvAgentCardRegistry({
    connection,
    bucket: `A2A_AGENT_CARDS_${Date.now()}`,
    namespace: 'server-a',
  });

  try {
    const card = agentCard('Integration Agent', 'nats://integration.a2a.agent.integration.rpc');
    const published = await registry.publish({ agentId: 'agent-a', card });
    const resolved = await registry.resolve('agent-a');
    const listed = await registry.list();

    assert.equal(published.key, 'server-a.agents.agent-a');
    assert.deepEqual(resolved?.card, card);
    assert.deepEqual(listed.map(entry => entry.key), ['server-a.agents.agent-a']);
  } finally {
    await connection.drain();
  }
});

class IntegrationRequestHandler implements A2ARequestHandler {
  private readonly agentCard = agentCard('Integration Agent', 'nats://integration.a2a.agent.integration.rpc');

  async getAgentCard(): Promise<AgentCard> {
    return this.agentCard;
  }

  async getAuthenticatedExtendedAgentCard(_params: GetExtendedAgentCardRequest, _context: ServerCallContext): Promise<AgentCard> {
    return this.agentCard;
  }

  async sendMessage(params: SendMessageRequest): Promise<Message> {
    return agentMessage(`echo: ${textFrom(params)}`);
  }

  async *sendMessageStream(): AsyncGenerator<StreamResponse, void, undefined> {
    yield { payload: { $case: 'task', value: task('task-real', TaskState.TASK_STATE_WORKING) } };
    yield {
      payload: {
        $case: 'statusUpdate',
        value: {
          taskId: 'task-real',
          contextId: 'context-real',
          status: { state: TaskState.TASK_STATE_COMPLETED, message: undefined, timestamp: undefined },
          metadata: undefined,
        },
      },
    };
  }

  async getTask(params: GetTaskRequest): Promise<Task> {
    return task(params.id, TaskState.TASK_STATE_COMPLETED);
  }

  async cancelTask(params: CancelTaskRequest): Promise<Task> {
    return task(params.id, TaskState.TASK_STATE_CANCELED);
  }

  async listTasks(_params: ListTasksRequest): Promise<ListTasksResponse> {
    return { tasks: [task('task-real', TaskState.TASK_STATE_COMPLETED)], nextPageToken: '', pageSize: 1, totalSize: 1 };
  }

  async createTaskPushNotificationConfig(params: TaskPushNotificationConfig): Promise<TaskPushNotificationConfig> {
    return params;
  }

  async getTaskPushNotificationConfig(params: GetTaskPushNotificationConfigRequest): Promise<TaskPushNotificationConfig> {
    return pushConfig(params.taskId, params.id);
  }

  async listTaskPushNotificationConfigs(
    params: ListTaskPushNotificationConfigsRequest
  ): Promise<ListTaskPushNotificationConfigsResponse> {
    return { configs: [pushConfig(params.taskId, 'config-real')], nextPageToken: '' };
  }

  async deleteTaskPushNotificationConfig(_params: DeleteTaskPushNotificationConfigRequest): Promise<void> {}

  async *resubscribe(params: SubscribeToTaskRequest): AsyncGenerator<StreamResponse, void, undefined> {
    yield { payload: { $case: 'task', value: task(params.id, TaskState.TASK_STATE_COMPLETED) } };
  }
}

function agentCard(name: string, url: string): AgentCard {
  return {
    name,
    description: 'A test agent backed by a real NATS server',
    version: '0.2.0',
    supportedInterfaces: [
      {
        url,
        protocolBinding: 'NATS',
        protocolVersion: A2A_PROTOCOL_VERSION,
        tenant: '',
      },
    ],
    provider: undefined,
    capabilities: {
      streaming: true,
      pushNotifications: true,
      extensions: [],
      extendedAgentCard: true,
    },
    securitySchemes: {},
    securityRequirements: [],
    defaultInputModes: ['text/plain'],
    defaultOutputModes: ['text/plain'],
    skills: [],
    signatures: [],
  };
}

function sendRequest(text: string): SendMessageRequest {
  return {
    tenant: '',
    message: {
      messageId: `message-${text}`,
      contextId: '',
      taskId: '',
      role: Role.ROLE_USER,
      parts: [{ content: { $case: 'text', value: text }, metadata: undefined, filename: '', mediaType: 'text/plain' }],
      metadata: undefined,
      extensions: [],
      referenceTaskIds: [],
    },
    configuration: undefined,
    metadata: undefined,
  };
}

function agentMessage(text: string): Message {
  return {
    messageId: `agent-${text}`,
    contextId: 'context-real',
    taskId: '',
    role: Role.ROLE_AGENT,
    parts: [{ content: { $case: 'text', value: text }, metadata: undefined, filename: '', mediaType: 'text/plain' }],
    metadata: undefined,
    extensions: [],
    referenceTaskIds: [],
  };
}

function task(id: string, state: TaskState): Task {
  return {
    id,
    contextId: 'context-real',
    status: { state, message: undefined, timestamp: undefined },
    artifacts: [],
    history: [],
    metadata: undefined,
  };
}

function pushConfig(taskId: string, id: string): TaskPushNotificationConfig {
  return {
    tenant: '',
    taskId,
    id,
    url: 'https://example.test/a2a',
    token: '',
    authentication: undefined,
  };
}

function assertMessage(result: SendMessageResult): Message {
  assert.ok('parts' in result);
  return result;
}

function messageText(message: Message): string {
  const part = message.parts[0];
  return part?.content?.$case === 'text' ? part.content.value : '';
}

function textFrom(params: SendMessageRequest): string {
  const part = params.message?.parts[0];
  return part?.content?.$case === 'text' ? part.content.value : '';
}
