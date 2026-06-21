import {
  A2A_NATS_PROTOCOL,
  NATS_JETSTREAM_TRANSPORT_PROTOCOL_NAME,
  NATS_TRANSPORT_PROTOCOL_NAME,
} from './subjects.js';

export const A2A_NATS_TRANSPORT_EXTENSION_URI = 'https://wyrd.company/a2a/extensions/nats-transport/v1';

export interface A2aNatsTransportExtensionOptions {
  readonly required?: boolean;
}

export interface A2aNatsTransportExtensionDeclaration {
  readonly uri: typeof A2A_NATS_TRANSPORT_EXTENSION_URI;
  readonly description: string;
  readonly required: boolean;
  readonly params: {
    readonly protocol: typeof A2A_NATS_PROTOCOL;
    readonly transports: readonly [
      typeof NATS_TRANSPORT_PROTOCOL_NAME,
      typeof NATS_JETSTREAM_TRANSPORT_PROTOCOL_NAME,
    ];
  };
}

export function a2aNatsTransportExtension(
  options: A2aNatsTransportExtensionOptions = {},
): A2aNatsTransportExtensionDeclaration {
  return {
    uri: A2A_NATS_TRANSPORT_EXTENSION_URI,
    description: 'Declares support for the Wyrd A2A NATS transport binding.',
    required: options.required ?? false,
    params: {
      protocol: A2A_NATS_PROTOCOL,
      transports: [
        NATS_TRANSPORT_PROTOCOL_NAME,
        NATS_JETSTREAM_TRANSPORT_PROTOCOL_NAME,
      ],
    },
  };
}
