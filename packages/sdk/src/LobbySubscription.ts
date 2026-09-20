import type { Room } from './Room.ts';
import { createNanoEvents } from './core/nanoevents.ts';

export const LobbySubscriptionStatus = {
  SYNCING: 'syncing',
  ACTIVE: 'active',
  RESYNCING: 'resyncing',
  ENDED: 'ended',
} as const;

export type LobbySubscriptionStatus = typeof LobbySubscriptionStatus[keyof typeof LobbySubscriptionStatus];

export type LobbySubscriptionEventAction =
  | 'create'
  | 'update'
  | 'lock'
  | 'unlock'
  | 'remove'
  | 'destroy';

export type LobbyFilterMetadataValue = string | number | boolean;

export interface LobbyFilter {
  name?: string;
  metadata?: { [field: string]: LobbyFilterMetadataValue };
}

export interface LobbySubscriptionOptions extends LobbyFilter {
  id?: string;
  cursor?: string;
}

export interface LobbyRoomListing {
  name: string;
  roomId: string;
  processId?: string;
  clients?: number;
  maxClients?: number;
  locked?: boolean;
  private?: boolean;
  publicAddress?: string;
  metadata?: any;
  createdAt?: string | Date;
}

export interface LobbySnapshot {
  id: string;
  epoch: string;
  seq: number;
  cursor: string;
  reason: string;
  rooms: LobbyRoomListing[];
}

export interface LobbySubscriptionEvent {
  id: string;
  seq: number;
  cursor: string;
  action: LobbySubscriptionEventAction;
  reason?: 'private' | 'destroyed';
  roomId: string;
  room?: LobbyRoomListing;
}

export interface LobbySubscriptionStatusMessage {
  id: string;
  status: LobbySubscriptionStatus;
  reason?: string;
}

type EventHandler<T extends any[]> = (...args: T) => void;

interface EventChannel<T extends any[]> {
  (handler: EventHandler<T>): () => void;
  on(handler: EventHandler<T>): () => void;
  remove(handler: EventHandler<T>): void;
  invoke(...args: T): void;
}

function createEventChannel<T extends any[]>(): EventChannel<T> {
  const emitter = createNanoEvents<{ event: EventHandler<T> }>();
  const channel = (handler: EventHandler<T>) => emitter.on('event', handler);
  channel.on = channel;
  channel.remove = (handler: EventHandler<T>) => {
    emitter.events.event = emitter.events.event?.filter((candidate) => candidate !== handler);
  };
  channel.invoke = (...args: T) => emitter.emit('event', ...args);
  return channel;
}

/**
 * Cursor-based helper for {@link LobbyRoom}. It applies the snapshot boundary
 * before any replay, checks the continuous projected sequence, and exposes the
 * same cursor on events and snapshots for explicit re-subscription.
 */
export class LobbySubscription {
  public readonly id: string;

  public status: LobbySubscriptionStatus = LobbySubscriptionStatus.SYNCING;
  public cursor: string | undefined;
  public readonly rooms: Map<string, LobbyRoomListing> = new Map();

  public onEvent = createEventChannel<[LobbySubscriptionEvent]>();
  public onSnapshot = createEventChannel<[LobbySnapshot]>();
  public onStatusChange = createEventChannel<[LobbySubscriptionStatus, reason?: string]>();
  public onEnd = createEventChannel<[reason?: string]>();

  protected room: Room;
  protected expectedSeq: number = 0;
  protected ended: boolean = false;
  protected resyncing: boolean = false;

  protected unbindMessage: () => void;
  protected unbindLeave: () => void;
  protected unbindReconnect: () => void;

  constructor(room: Room, options: LobbySubscriptionOptions = {}) {
    this.room = room;
    this.id = options.id ?? 'default';
    this.cursor = options.cursor;

    const unbindEvent = room.onMessage('lobby:event', (event: LobbySubscriptionEvent) => this.applyEvent(event));
    const unbindSnapshot = room.onMessage('lobby:snapshot', (snapshot: LobbySnapshot) => this.applySnapshot(snapshot));
    const unbindStatus = room.onMessage('lobby:status', (message: LobbySubscriptionStatusMessage) => this.applyStatus(message));

    // The server keeps the legacy lobby messages for compatibility. Registering
    // no-op handlers prevents those boundary messages from becoming SDK warnings.
    const unbindRooms = room.onMessage('rooms', () => {});
    const unbindAdd = room.onMessage('+', () => {});
    const unbindRemove = room.onMessage('-', () => {});

    this.unbindMessage = () => {
      unbindEvent();
      unbindSnapshot();
      unbindStatus();
      unbindRooms();
      unbindAdd();
      unbindRemove();
    };

    const handleLeave = () => this.end('connection-left');
    const handleReconnect = () => {
      // The server resumes the same subscription on a same-room reconnect. Mark
      // the boundary locally while the replay/snapshot is in flight.
      this.setStatus(LobbySubscriptionStatus.RESYNCING, 'reconnect');
    };
    room.onLeave(handleLeave);
    room.onReconnect(handleReconnect);
    this.unbindLeave = () => room.onLeave.remove(handleLeave);
    this.unbindReconnect = () => room.onReconnect.remove(handleReconnect);

    room.send('lobby:subscribe', {
      id: this.id,
      cursor: options.cursor,
      name: options.name,
      metadata: options.metadata,
    } satisfies LobbySubscriptionOptions);
  }

  public resync(filter?: LobbyFilter): void {
    if (this.ended) { return; }
    this.resyncing = true;
    this.setStatus(LobbySubscriptionStatus.RESYNCING, 'resync');
    this.room.send('lobby:resync', { id: this.id, filter });
  }

  public unsubscribe(): void {
    if (this.ended) { return; }
    this.room.send('lobby:unsubscribe', this.id);
  }

  public leave(): void {
    this.unsubscribe();
  }

  public dispose(): void {
    this.unbindMessage?.();
    this.unbindLeave?.();
    this.unbindReconnect?.();
  }

  protected applySnapshot(snapshot: LobbySnapshot): void {
    if (this.ended || snapshot.id !== this.id) { return; }

    if (snapshot.seq < this.expectedSeq || (!this.resyncing && snapshot.seq !== this.expectedSeq)) {
      // A stale boundary must not replace newer local state. A future boundary
      // during an active resync already contains the missing final states.
      if (snapshot.seq < this.expectedSeq && !this.resyncing) {
        this.resync();
      }
      return;
    }

    this.rooms.clear();
    for (const room of snapshot.rooms) {
      this.rooms.set(room.roomId, room);
    }

    this.cursor = snapshot.cursor;
    this.expectedSeq = snapshot.seq + 1;
    this.resyncing = false;
    this.onSnapshot.invoke(snapshot);
  }

  protected applyEvent(event: LobbySubscriptionEvent): void {
    if (this.ended || event.id !== this.id || event.roomId === undefined) { return; }

    if (event.seq < this.expectedSeq) {
      return;
    }

    if (event.seq !== this.expectedSeq) {
      if (!this.resyncing) {
        this.resync();
      }
      return;
    }

    switch (event.action) {
      case 'destroy':
      case 'remove':
        this.rooms.delete(event.roomId);
        break;

      case 'create':
      case 'update':
      case 'lock':
      case 'unlock':
        if (!event.room) { return; }
        this.rooms.set(event.roomId, event.room);
        break;
    }

    this.cursor = event.cursor;
    this.expectedSeq++;
    if (!this.resyncing) {
      this.setStatus(LobbySubscriptionStatus.ACTIVE);
    }
    this.onEvent.invoke(event);
  }

  protected applyStatus(message: LobbySubscriptionStatusMessage): void {
    if (message.id !== this.id) { return; }

    if (message.status === LobbySubscriptionStatus.ENDED) {
      this.end(message.reason);
      return;
    }

    this.setStatus(message.status, message.reason);
  }

  protected setStatus(status: LobbySubscriptionStatus, reason?: string): void {
    if (this.status === status && status !== LobbySubscriptionStatus.RESYNCING) { return; }
    this.status = status;
    this.resyncing = status === LobbySubscriptionStatus.RESYNCING;
    this.onStatusChange.invoke(status, reason);
  }

  protected end(reason?: string): void {
    if (this.ended) { return; }
    this.ended = true;
    this.resyncing = false;
    this.status = LobbySubscriptionStatus.ENDED;
    this.onStatusChange.invoke(this.status, reason);
    this.onEnd.invoke(reason);
    this.dispose();
  }
}
