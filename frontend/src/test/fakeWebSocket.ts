import type { ClientMessage, ServerMessage } from '../types';

export class FakeWebSocket {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSING = 2;
  static readonly CLOSED = 3;

  static instances: FakeWebSocket[] = [];

  readonly url: string | URL;
  readonly sentMessages: ClientMessage[] = [];
  readyState = FakeWebSocket.CONNECTING;
  onclose: ((event: CloseEvent) => void) | null = null;
  onerror: ((event: Event) => void) | null = null;
  onmessage: ((event: MessageEvent<string>) => void) | null = null;
  onopen: ((event: Event) => void) | null = null;

  constructor(url: string | URL) {
    this.url = url;
    FakeWebSocket.instances.push(this);
  }

  static reset() {
    FakeWebSocket.instances = [];
  }

  open() {
    this.readyState = FakeWebSocket.OPEN;
    this.onopen?.(new Event('open'));
  }

  send(payload: string) {
    this.sentMessages.push(JSON.parse(payload) as ClientMessage);
  }

  receive(message: ServerMessage) {
    this.onmessage?.(
      new MessageEvent('message', {
        data: JSON.stringify(message),
      })
    );
  }

  receiveRaw(payload: string) {
    this.onmessage?.(
      new MessageEvent('message', {
        data: payload,
      })
    );
  }

  emitError() {
    this.onerror?.(new Event('error'));
  }

  close() {
    this.readyState = FakeWebSocket.CLOSED;
    this.onclose?.(new CloseEvent('close'));
  }
}
