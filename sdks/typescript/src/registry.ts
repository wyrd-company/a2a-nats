import type { AgentCard } from '@a2a-js/sdk';
import { Kvm, type KV } from '@nats-io/kv';
import type { NatsConnection } from '@nats-io/transport-node';

import {
  a2aNatsAgentCardKey,
  a2aNatsAgentCardNamespaceFilter,
} from '../../../protocol/src/index.js';
import { decodeJson, encodeJson } from './codec.js';

export interface AgentCardRegistryOptions {
  readonly connection: NatsConnection;
  readonly bucket: string;
  readonly namespace: string;
  readonly createBucket?: boolean;
}

export interface PublishAgentCardOptions {
  readonly agentId: string;
  readonly card: AgentCard;
}

export interface AgentCardRegistryEntry {
  readonly key: string;
  readonly namespace: string;
  readonly agentId: string;
  readonly revision: number;
  readonly card: AgentCard;
}

export class JetStreamKvAgentCardRegistry {
  private readonly bucketPromise: Promise<KV>;

  constructor(private readonly options: AgentCardRegistryOptions) {
    const kvm = new Kvm(options.connection);
    this.bucketPromise =
      options.createBucket === false ? kvm.open(options.bucket) : kvm.create(options.bucket);
  }

  async publish(options: PublishAgentCardOptions): Promise<AgentCardRegistryEntry> {
    const bucket = await this.bucketPromise;
    const key = this.keyFor(options.agentId);
    const revision = await bucket.put(key, encodeJson(options.card));
    return {
      key,
      namespace: this.options.namespace,
      agentId: options.agentId,
      revision,
      card: options.card,
    };
  }

  async resolve(agentId: string): Promise<AgentCardRegistryEntry | undefined> {
    const bucket = await this.bucketPromise;
    const key = this.keyFor(agentId);
    const entry = await bucket.get(key);
    if (!entry || entry.operation === 'DEL' || entry.operation === 'PURGE') {
      return undefined;
    }
    return {
      key,
      namespace: this.options.namespace,
      agentId,
      revision: entry.revision,
      card: decodeJson(entry.value) as AgentCard,
    };
  }

  async list(): Promise<AgentCardRegistryEntry[]> {
    const bucket = await this.bucketPromise;
    const keys = await bucket.keys(a2aNatsAgentCardNamespaceFilter(this.options.namespace));
    const entries: AgentCardRegistryEntry[] = [];
    for await (const key of keys) {
      const agentId = this.agentIdFromKey(key);
      if (!agentId) {
        continue;
      }
      const entry = await this.resolve(agentId);
      if (entry) {
        entries.push(entry);
      }
    }
    return entries;
  }

  keyFor(agentId: string): string {
    return a2aNatsAgentCardKey({
      namespace: this.options.namespace,
      agentId,
    });
  }

  private agentIdFromKey(key: string): string | undefined {
    const prefix = `${this.options.namespace}.agents.`;
    return key.startsWith(prefix) ? key.slice(prefix.length) : undefined;
  }
}

export function createJetStreamKvAgentCardRegistry(
  options: AgentCardRegistryOptions
): JetStreamKvAgentCardRegistry {
  return new JetStreamKvAgentCardRegistry(options);
}
