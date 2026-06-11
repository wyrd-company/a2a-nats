export const A2A_NATS_PROTOCOL = 'a2a-nats/1';
export const DEFAULT_A2A_NATS_NAMESPACE = 'a2a';
export const NATS_TRANSPORT_PROTOCOL_NAME = 'NATS';
export const NATS_JETSTREAM_TRANSPORT_PROTOCOL_NAME = 'NATS+JS';

export interface A2ANatsAgentSubjectOptions {
  readonly namespace?: string;
  readonly agentId: string;
}

export interface A2AJetStreamRequestSubjectOptions {
  readonly namespace?: string;
  readonly agentId: string;
}

export interface A2AJetStreamResponseSubjectOptions {
  readonly namespace?: string;
  readonly clientId: string;
}

export interface A2ANatsAgentCardKeyOptions {
  readonly namespace: string;
  readonly agentId: string;
}

export function a2aNatsAgentSubject(options: A2ANatsAgentSubjectOptions): string {
  const namespace = sanitizeSubjectToken(options.namespace ?? DEFAULT_A2A_NATS_NAMESPACE);
  const agentId = sanitizeSubjectToken(options.agentId);
  return `${namespace}.agent.${agentId}.rpc`;
}

export function a2aJetStreamRequestSubject(options: A2AJetStreamRequestSubjectOptions): string {
  const namespace = sanitizeSubjectToken(options.namespace ?? DEFAULT_A2A_NATS_NAMESPACE);
  const agentId = sanitizeSubjectToken(options.agentId);
  return `${namespace}.agent.${agentId}.requests`;
}

export function a2aJetStreamResponseSubject(options: A2AJetStreamResponseSubjectOptions): string {
  const namespace = sanitizeSubjectToken(options.namespace ?? DEFAULT_A2A_NATS_NAMESPACE);
  const clientId = sanitizeSubjectToken(options.clientId);
  return `${namespace}.client.${clientId}.responses`;
}

export function a2aNatsAgentCardKey(options: A2ANatsAgentCardKeyOptions): string {
  const namespace = sanitizeSubjectToken(options.namespace);
  const agentId = sanitizeSubjectToken(options.agentId);
  return `${namespace}.agents.${agentId}`;
}

export function a2aNatsAgentCardNamespaceFilter(namespace: string): string {
  return `${sanitizeSubjectToken(namespace)}.agents.*`;
}

export function natsSubjectFromAgentUrl(url: string): string {
  const trimmed = url.trim();
  if (!trimmed) {
    throw new Error('NATS agent URL cannot be empty');
  }

  if (!trimmed.includes('://')) {
    return trimmed;
  }

  const parsed = new URL(trimmed);
  if (parsed.protocol !== 'nats:' && parsed.protocol !== 'nats+js:') {
    throw new Error(`Unsupported A2A NATS URL protocol: ${parsed.protocol}`);
  }

  const path = parsed.pathname.replace(/^\/+|\/+$/g, '').replaceAll('/', '.');
  const subject = [parsed.hostname, path].filter(Boolean).join('.');
  if (!subject) {
    throw new Error('NATS agent URL does not contain a subject');
  }
  return subject;
}

export function sanitizeSubjectToken(value: string): string {
  const token = value.trim().replaceAll(/[^A-Za-z0-9_-]/g, '_');
  if (!token) {
    throw new Error('NATS subject token cannot be empty');
  }
  return token;
}
