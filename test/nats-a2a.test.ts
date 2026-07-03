import assert from 'node:assert/strict';
import { test } from 'node:test';

import {
  A2A_PROTOCOL_VERSION,
  A2A_VERSION_HEADER,
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
import { TaskNotFoundError } from '@a2a-js/sdk/client';
import type { A2ARequestHandler, ServerCallContext } from '@a2a-js/sdk/server';

import {
  A2A_NATS_PROTOCOL,
  A2A_NATS_TRANSPORT_EXTENSION_URI,
  NATS_JETSTREAM_TRANSPORT_PROTOCOL_NAME,
  NATS_TRANSPORT_PROTOCOL_NAME,
  NatsA2AClientTransport,
  NatsA2AServer,
  a2aJetStreamRequestSubject,
  a2aJetStreamResponseSubject,
  a2aNatsAgentCardKey,
  a2aNatsAgentCardNamespaceFilter,
  a2aNatsAgentSubject,
  a2aNatsTransportExtension,
  natsSubjectFromAgentUrl,
} from '../sdks/typescript/src/index.js';
import {
  decodeRequestFrame,
  decodeRequestPayload,
  decodeStreamFrame,
  encodeRequestFrame,
  encodeStreamResponseFrame,
} from '../sdks/typescript/src/codec.js';
import { FakeNatsBroker } from './fake-nats.js';

test('encodes canonical v1 sendMessage and stream payload frames', () => {
  const request = sendRequest('canonical');
  const decodedRequestFrame = decodeRequestFrame(encodeRequestFrame('sendMessage', request));
  const decodedRequest = decodeRequestPayload<SendMessageRequest>(decodedRequestFrame);

  assert.equal(decodedRequestFrame.protocol, 'a2a-nats/2');
  assert.equal(decodedRequestFrame.method, 'sendMessage');
  assert.deepEqual(SendMessageRequest.toJSON(decodedRequest), SendMessageRequest.toJSON(request));

  const streamResponse: StreamResponse = {
    payload: { $case: 'message', value: agentMessage('stream canonical') },
  };
  const decodedStreamFrame = decodeStreamFrame(encodeStreamResponseFrame(streamResponse, 'stream-1'));

  assert.equal(decodedStreamFrame.kind, 'response');
  assert.deepEqual(StreamResponse.fromJSON(decodedStreamFrame.payload), streamResponse);
});

test('routes unary A2A v1 canonical calls over NATS', async () => {
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

  const result = await client.sendMessage(sendRequest('hello'));

  assert.equal(subject, 'wyrd_a2a.agent.agent-a.rpc');
  assert.equal(messageText(assertMessage(result)), 'echo: hello');
  assert.equal(handler.lastContext?.requestedVersion, A2A_PROTOCOL_VERSION);

  server.close();
});

test('routes streaming A2A v1 StreamResponse events over a NATS reply inbox', async () => {
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
  for await (const event of client.sendMessageStream(sendRequest('stream'))) {
    events.push(event.payload?.$case);
  }

  assert.deepEqual(events, ['task', 'statusUpdate']);

  server.close();
});

test('sends A2A-Version as a NATS header and surfaces it server-side', async () => {
  const broker = new FakeNatsBroker();
  const subject = a2aNatsAgentSubject({ agentId: 'header-agent' });
  const handler = new StubRequestHandler();
  const server = new NatsA2AServer({ connection: broker, subject, requestHandler: handler });
  const client = new NatsA2AClientTransport({
    connection: broker,
    subject,
    createInbox: () => broker.createInbox(),
  });

  await server.ready();
  await client.sendMessage(sendRequest('headers'), {
    serviceParameters: {
      [A2A_VERSION_HEADER]: '0.3',
      'A2A-Extensions': 'https://example.test/extensions/demo',
    },
  });

  assert.equal(handler.lastContext?.requestedVersion, A2A_PROTOCOL_VERSION);
  assert.deepEqual(handler.lastContext?.requestedExtensions, ['https://example.test/extensions/demo']);

  server.close();
});

test('rethrows typed A2A errors across the NATS wire', async () => {
  const broker = new FakeNatsBroker();
  const subject = a2aNatsAgentSubject({ agentId: 'error-agent' });
  const server = new NatsA2AServer({
    connection: broker,
    subject,
    requestHandler: new StubRequestHandler({ throwTaskNotFound: true }),
  });
  const client = new NatsA2AClientTransport({
    connection: broker,
    subject,
    createInbox: () => broker.createInbox(),
  });

  await server.ready();
  await assert.rejects(() => client.sendMessage(sendRequest('missing')), TaskNotFoundError);

  server.close();
});

test('parses NATS agent URLs and exposes protocol names', () => {
  assert.equal(A2A_NATS_PROTOCOL, 'a2a-nats/2');
  assert.equal(NATS_TRANSPORT_PROTOCOL_NAME, 'NATS');
  assert.equal(NATS_JETSTREAM_TRANSPORT_PROTOCOL_NAME, 'NATS+JS');
  assert.equal(natsSubjectFromAgentUrl('nats://a2a.agent.agent-a.rpc'), 'a2a.agent.agent-a.rpc');
  assert.equal(natsSubjectFromAgentUrl('nats+js://a2a/agent/agent-a/requests'), 'a2a.agent.agent-a.requests');
  assert.equal(natsSubjectFromAgentUrl('nats://a2a/agent/agent-a/rpc'), 'a2a.agent.agent-a.rpc');
  assert.equal(natsSubjectFromAgentUrl('a2a.agent.agent-a.rpc'), 'a2a.agent.agent-a.rpc');
  assert.equal(
    a2aJetStreamRequestSubject({ namespace: 'server-a', agentId: 'agent-a' }),
    'server-a.agent.agent-a.requests'
  );
  assert.equal(
    a2aJetStreamResponseSubject({ namespace: 'server-a', clientId: 'client-a' }),
    'server-a.client.client-a.responses'
  );
  assert.equal(
    a2aNatsAgentCardKey({ namespace: 'server-a', agentId: 'agent-a' }),
    'server-a.agents.agent-a'
  );
  assert.equal(a2aNatsAgentCardNamespaceFilter('server-a'), 'server-a.agents.*');
});

test('declares the A2A NATS transport extension', () => {
  const extension = a2aNatsTransportExtension();

  assert.equal(extension.uri, A2A_NATS_TRANSPORT_EXTENSION_URI);
  assert.equal(extension.required, false);
  assert.equal(extension.params.protocol, A2A_NATS_PROTOCOL);
  assert.deepEqual(extension.params.transports, ['NATS', 'NATS+JS']);
});

class StubRequestHandler implements A2ARequestHandler {
  lastContext: ServerCallContext | undefined;

  private readonly agentCard = agentCard('Stub Agent', 'nats://a2a.agent.stub.rpc');

  constructor(private readonly options: { throwTaskNotFound?: boolean } = {}) {}

  async getAgentCard(): Promise<AgentCard> {
    return this.agentCard;
  }

  async getAuthenticatedExtendedAgentCard(_params: GetExtendedAgentCardRequest, context: ServerCallContext): Promise<AgentCard> {
    this.lastContext = context;
    return this.agentCard;
  }

  async sendMessage(params: SendMessageRequest, context: ServerCallContext): Promise<Message> {
    this.lastContext = context;
    if (this.options.throwTaskNotFound) {
      throw new TaskNotFoundError('Task missing');
    }
    return agentMessage(`echo: ${textFrom(params)}`);
  }

  async *sendMessageStream(_params: SendMessageRequest, context: ServerCallContext): AsyncGenerator<StreamResponse, void, undefined> {
    this.lastContext = context;
    yield { payload: { $case: 'task', value: task('task-1', TaskState.TASK_STATE_WORKING) } };
    yield {
      payload: {
        $case: 'statusUpdate',
        value: {
          taskId: 'task-1',
          contextId: 'context-1',
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
    return { tasks: [task('task-1', TaskState.TASK_STATE_COMPLETED)], nextPageToken: '', pageSize: 1, totalSize: 1 };
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
    return { configs: [pushConfig(params.taskId, 'config-1')], nextPageToken: '' };
  }

  async deleteTaskPushNotificationConfig(_params: DeleteTaskPushNotificationConfigRequest): Promise<void> {}

  async *resubscribe(params: SubscribeToTaskRequest): AsyncGenerator<StreamResponse, void, undefined> {
    yield { payload: { $case: 'task', value: task(params.id, TaskState.TASK_STATE_COMPLETED) } };
  }
}

function agentCard(name: string, url: string): AgentCard {
  return {
    name,
    description: 'A test agent',
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
    contextId: 'context-1',
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
    contextId: 'context-1',
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
