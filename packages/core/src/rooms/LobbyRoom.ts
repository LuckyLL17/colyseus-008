import { CloseCode } from '@colyseus/shared-types';

import * as matchMaker from '../MatchMaker.ts';
import type { IRoomCache } from '../matchmaker/LocalDriver/LocalDriver.ts';
import type { Client } from '../Transport.ts';
import { LOBBY_ACTION, subscribeLobby, type LobbyAction, type LobbyChange } from '../matchmaker/Lobby.ts';
import { generateId } from '../utils/Utils.ts';
import { debugAndPrintError } from '../Debug.ts';
import { Room } from '../Room.ts';

// TODO: use Schema state & filters on version 1.0.0

// class DummyLobbyState extends Schema { // tslint:disable-line
//   @type("number") public _: number;
// }

export const LOBBY_SUBSCRIPTION_STATUS = {
  SYNCING: 'syncing',
  ACTIVE: 'active',
  RESYNCING: 'resyncing',
  ENDED: 'ended',
} as const;

export type LobbySubscriptionStatus = typeof LOBBY_SUBSCRIPTION_STATUS[keyof typeof LOBBY_SUBSCRIPTION_STATUS];

export type LobbySubscriptionEventAction =
  | 'create'
  | 'update'
  | 'lock'
  | 'unlock'
  | 'remove'
  | 'destroy';

export type LobbyFilterMetadataValue = string | number | boolean;

export interface FilterInput {
  name?: string;
  metadata?: { [field: string]: LobbyFilterMetadataValue };
}

export interface LobbySubscribeOptions extends FilterInput {
  id?: string;
  cursor?: string;
}

export interface LobbyOptions {
  filter?: FilterInput;

  /**
   * How long, in seconds, a dropped client can resume the same lobby
   * subscription. Set to 0 to disable lobby-level reconnection.
   */
  reconnectionTime?: number;

  /** Number of global lobby changes retained for cursor-based replay. */
  eventHistorySize?: number;

  /** Maximum global events resumed before choosing a fresh snapshot. */
  maxBufferedEvents?: number;

  /** Transport send-buffer size that forces a slow client to resynchronize. */
  maxBufferedBytes?: number;
}

export interface LobbySnapshot<Metadata = any> {
  id: string;
  epoch: string;
  seq: number;
  cursor: string;
  reason: string;
  rooms: IRoomCache<Metadata>[];
}

export interface LobbySubscriptionEvent<Metadata = any> {
  id: string;
  seq: number;
  cursor: string;
  action: LobbySubscriptionEventAction;
  reason?: 'private' | 'destroyed';
  roomId: string;
  room?: IRoomCache<Metadata>;
}

export interface LobbySubscriptionStatusMessage {
  id: string;
  status: LobbySubscriptionStatus;
  reason?: string;
}

//
// Strongly-typed client messages for LobbyRoom
// (This is optional, but recommended for better type safety and code generation for native SDKs)
//
type LobbyClient = Client<{
  messages: {
    rooms: IRoomCache[];
    '+': [roomId: string, room: IRoomCache];
    '-': string;
    filter: FilterInput;
    'lobby:subscribe': LobbySubscribeOptions;
    'lobby:unsubscribe': string;
    'lobby:resync': { id: string, filter?: FilterInput };
    'lobby:snapshot': LobbySnapshot;
    'lobby:event': LobbySubscriptionEvent;
    'lobby:status': LobbySubscriptionStatusMessage;
  }
}>;

export type LobbyMessages = {
  filter: (client: LobbyClient, filter: FilterInput) => void;
  'lobby:subscribe': (client: LobbyClient, options: LobbySubscribeOptions) => void;
  'lobby:unsubscribe': (client: LobbyClient, id: string) => void;
  'lobby:resync': (client: LobbyClient, options: { id: string, filter?: FilterInput }) => void;
};

interface LobbyEventHistoryEntry<Metadata = any> {
  seq: number;
  roomId: string;
  action: LobbyAction;
  reason?: 'private' | 'destroyed';
  before?: IRoomCache<Metadata>;
  after?: IRoomCache<Metadata>;
}

interface LobbySubscriptionState {
  id: string;
  sessionId: string;
  filter?: FilterInput;
  seq: number;
  globalSeq: number;
  active: boolean;
}

interface ResolvedLobbyEvent {
  action: LobbySubscriptionEventAction;
  reason?: 'private' | 'destroyed';
}

export class LobbyRoom<Metadata = any> extends Room {
  public rooms: IRoomCache<Metadata>[] = [];
  public unsubscribeLobby: () => void;

  public clientOptions: { [sessionId: string]: LobbyOptions } = {};

  public epoch: string = generateId();
  public eventHistorySize: number = 1000;
  public maxBufferedEvents: number = 256;
  public maxBufferedBytes: number = 1024 * 1024;
  public reconnectionTime: number = 20;

  public eventHistory: LobbyEventHistoryEntry<Metadata>[] = [];
  public globalEventSeq: number = 0;
  protected subscriptions: Map<string, LobbySubscriptionState> = new Map();
  protected subscriptionKeys: Map<string, Set<string>> = new Map();
  protected changeQueue: Promise<void> = Promise.resolve();

  declare messages: LobbyMessages;

  public async onCreate(options: LobbyOptions = {}) {
    // prevent LobbyRoom to notify itself
    this['_listing'].unlisted = true;

    this.eventHistorySize = options.eventHistorySize ?? this.eventHistorySize;
    this.maxBufferedEvents = options.maxBufferedEvents ?? this.maxBufferedEvents;
    this.maxBufferedBytes = options.maxBufferedBytes ?? this.maxBufferedBytes;
    this.reconnectionTime = options.reconnectionTime ?? this.reconnectionTime;

    this.onMessage('filter', (client, filter) => {
      const clientOptions = this.clientOptions[client.sessionId];
      if (!clientOptions) { return; }

      clientOptions.filter = filter;
      client.send('rooms', this.filterItemsForClient(clientOptions));
    });

    this.onMessage('lobby:subscribe', (client, request = {}) => {
      const clientOptions = this.clientOptions[client.sessionId];
      if (!clientOptions) { return; }

      const id = this.normalizeSubscriptionId(request.id);
      const key = this.getSubscriptionKey(client.sessionId, id);
      let subscription = this.subscriptions.get(key);

      if (subscription?.active) {
        subscription.filter = this.sanitizeFilter(request.filter ?? subscription.filter ?? clientOptions.filter);
        this.startSubscription(client, subscription, subscription.filter === undefined ? 'resync' : 'filter-changed');
        return;
      }

      subscription = {
        id,
        sessionId: client.sessionId,
        filter: this.sanitizeFilter(request.filter ?? clientOptions.filter),
        seq: 0,
        globalSeq: 0,
        active: true,
      };
      this.subscriptions.set(key, subscription);
      this.trackSubscription(client.sessionId, id);
      this.startSubscription(client, subscription, 'initial', request.cursor);
    });

    this.onMessage('lobby:unsubscribe', (client, id) => {
      const key = this.getSubscriptionKey(client.sessionId, this.normalizeSubscriptionId(id));
      const subscription = this.subscriptions.get(key);
      if (!subscription?.active) { return; }

      this.deleteSubscription(client.sessionId, subscription.id);
      client.send('lobby:status', { id: subscription.id, status: LOBBY_SUBSCRIPTION_STATUS.ENDED, reason: 'unsubscribed' });
    });

    this.onMessage('lobby:resync', (client, request) => {
      const key = this.getSubscriptionKey(client.sessionId, this.normalizeSubscriptionId(request?.id));
      const subscription = this.subscriptions.get(key);
      if (!subscription?.active) { return; }

      if (request.filter !== undefined) {
        subscription.filter = this.sanitizeFilter(request.filter);
      }
      this.startSubscription(client, subscription, request.filter ? 'filter-changed' : 'resync');
    });

    const roomsQuery = matchMaker.query<Room>({ private: false, unlisted: false });
    this.unsubscribeLobby = await subscribeLobby((roomId, data, change) => {
      this.changeQueue = this.changeQueue
        .then(() => roomsQuery.catch(() => undefined))
        .then(() => this.applyLobbyChange(roomId, data, change))
        .catch((e) => debugAndPrintError(e));
    });

    // Subscribe before awaiting the query. Presence callbacks wait on this same
    // query so their changes are applied to the snapshot instead of overwriting it.
    this.rooms = await roomsQuery;
    await this.changeQueue;
  }

  public onJoin(client: LobbyClient, options: LobbyOptions) {
    this.clientOptions[client.sessionId] = (
      !Array.isArray(options) && // Defold (Lua) sends empty objects as Array instead of object
      options
    ) || {};
    client.send('rooms', this.filterItemsForClient(this.clientOptions[client.sessionId]));
  }

  public async onLeave(client: LobbyClient, code?: number) {
    if (code !== CloseCode.CONSENTED && this.reconnectionTime > 0) {
      try {
        await this.allowReconnection(client, this.reconnectionTime);
        return;
      } catch (e) {
        // The reconnection window expired; clean the subscription below.
      }
    }

    this.removeClientSubscriptions(client.sessionId);
  }

  public onReconnect(client: LobbyClient) {
    const ids = this.subscriptionKeys.get(client.sessionId);
    if (!ids) { return; }

    for (const id of ids) {
      const subscription = this.subscriptions.get(this.getSubscriptionKey(client.sessionId, id));
      if (subscription?.active) {
        this.startSubscription(client, subscription, 'reconnect');
      }
    }
  }

  public onDispose() {
    for (const subscription of this.subscriptions.values()) {
      if (!subscription.active) { continue; }
      const client = this.clients.get(subscription.sessionId);
      client?.send('lobby:status', {
        id: subscription.id,
        status: LOBBY_SUBSCRIPTION_STATUS.ENDED,
        reason: 'disposed',
      });
    }

    if (this.unsubscribeLobby) {
      this.unsubscribeLobby();
    }
  }

  protected filterItemsForClient(options: LobbyOptions): IRoomCache<Metadata>[] {
    const filter = options.filter;

    return (filter)
      ? this.rooms.filter((room) => this.filterItemForClient(room, filter))
      : this.rooms;
  }

  protected filterItemForClient(room: IRoomCache, filter?: LobbyOptions['filter']) {
    if (!filter) {
      return true;
    }

    if (filter.name !== undefined && filter.name !== room.name) {
      return false;
    }

    if (filter.metadata) {
      for (const field in filter.metadata) {
        const metadata = room.metadata || {};
        if (metadata[field] !== filter.metadata[field]) {
          return false;
        }
      }
    }

    return true;
  }

  protected sanitizeFilter(filter?: FilterInput): FilterInput | undefined {
    if (!filter) {
      return undefined;
    }

    const sanitized: FilterInput = {};

    if (typeof filter.name === 'string' && filter.name.length > 0) {
      sanitized.name = filter.name;
    }

    if (filter.metadata && typeof filter.metadata === 'object') {
      const metadata: FilterInput['metadata'] = {};
      let count = 0;

      for (const field in filter.metadata) {
        if (count >= 8) { break; }
        const value = filter.metadata[field];
        if (value === undefined || typeof value === 'object' ||
          (typeof value === 'number' && !Number.isFinite(value))) { continue; }
        metadata[field] = value;
        count++;
      }

      if (count > 0) {
        sanitized.metadata = metadata;
      }
    }

    return (sanitized.name !== undefined || sanitized.metadata !== undefined)
      ? sanitized
      : undefined;
  }

  protected normalizeSubscriptionId(id?: string): string {
    if (typeof id === 'string' && /^[a-zA-Z0-9_-]{1,64}$/.test(id)) {
      return id;
    }
    return 'default';
  }

  protected getSubscriptionKey(sessionId: string, id: string): string {
    return `${sessionId}:${id}`;
  }

  protected trackSubscription(sessionId: string, id: string) {
    let ids = this.subscriptionKeys.get(sessionId);
    if (!ids) {
      ids = new Set();
      this.subscriptionKeys.set(sessionId, ids);
    }
    ids.add(id);
  }

  protected deleteSubscription(sessionId: string, id: string) {
    this.subscriptions.delete(this.getSubscriptionKey(sessionId, id));
    const ids = this.subscriptionKeys.get(sessionId);
    if (ids) {
      ids.delete(id);
      if (ids.size === 0) {
        this.subscriptionKeys.delete(sessionId);
      }
    }
  }

  protected formatCursor(globalSeq: number): string {
    return `${this.epoch}:${globalSeq}`;
  }

  protected parseCursor(cursor?: string): number | undefined {
    if (typeof cursor !== 'string' || cursor.length === 0) {
      return undefined;
    }

    const [epoch, seqText] = cursor.split(':');
    const seq = Number(seqText);

    if (epoch !== this.epoch || !Number.isSafeInteger(seq) || seq < 0) {
      return undefined;
    }

    return seq;
  }

  protected startSubscription(
    client: LobbyClient,
    subscription: LobbySubscriptionState,
    reason: string,
    cursor?: string,
  ) {
    const useStoredCursor = cursor === undefined && reason === 'reconnect';
    const cursorSeq = (cursor !== undefined)
      ? this.parseCursor(cursor)
      : (useStoredCursor ? subscription.globalSeq : 0);
    const oldestSeq = this.eventHistory[0]?.seq;

    const hasCursor = cursor !== undefined || useStoredCursor;
    const cursorIsValid = hasCursor &&
      cursorSeq !== undefined &&
      cursorSeq <= this.globalEventSeq &&
      (oldestSeq === undefined ? cursorSeq === 0 : cursorSeq >= oldestSeq);

    if (hasCursor && !cursorIsValid) {
      reason = (reason === 'reconnect') ? 'reconnect' : 'invalid-cursor';
    }

    if (!hasCursor || !cursorIsValid) {
      this.sendSnapshot(
        client,
        subscription,
        hasCursor ? reason : 'initial',
      );
      this.sendStatus(client, subscription, LOBBY_SUBSCRIPTION_STATUS.ACTIVE);
      return;
    }

    this.sendStatus(client, subscription, LOBBY_SUBSCRIPTION_STATUS.RESYNCING, reason);

    let replayCount = 0;
    for (const event of this.eventHistory) {
      if (event.seq <= cursorSeq) { continue; }

      const projected = this.projectEvent(event, subscription.filter);
      if (!projected) { continue; }

      replayCount++;
      if (replayCount > this.maxBufferedEvents) {
        this.sendSnapshot(client, subscription, 'backlog');
        this.sendStatus(client, subscription, LOBBY_SUBSCRIPTION_STATUS.ACTIVE);
        return;
      }

      subscription.seq++;
      subscription.globalSeq = event.seq;
      this.sendSubscriptionEvent(client, subscription, event, projected);
    }

    this.sendStatus(client, subscription, LOBBY_SUBSCRIPTION_STATUS.ACTIVE, reason === 'reconnect' ? 'replay' : undefined);
  }

  protected sendSnapshot(client: LobbyClient, subscription: LobbySubscriptionState, reason: string) {
    const rooms = this.filterItemsForClient({ filter: subscription.filter });
    subscription.globalSeq = this.globalEventSeq;
    const initial = reason === 'initial' || subscription.seq === 0;
    if (subscription.seq > 0) {
      subscription.seq++;
    }

    this.sendStatus(client, subscription, initial
      ? LOBBY_SUBSCRIPTION_STATUS.SYNCING
      : LOBBY_SUBSCRIPTION_STATUS.RESYNCING, reason);
    client.send('lobby:snapshot', {
      id: subscription.id,
      epoch: this.epoch,
      seq: subscription.seq,
      cursor: this.formatCursor(subscription.globalSeq),
      reason,
      rooms,
    } satisfies LobbySnapshot<Metadata>);

    // Preserve the legacy full-list boundary for clients using "+" and "-".
    client.send('rooms', rooms);
  }

  protected sendStatus(
    client: LobbyClient,
    subscription: LobbySubscriptionState,
    status: LobbySubscriptionStatus,
    reason?: string,
  ) {
    client.send('lobby:status', { id: subscription.id, status, reason });
  }

  protected applyLobbyChange(roomId: string, data: IRoomCache<Metadata> | null, change: LobbyChange): void {
    const roomIndex = this.rooms.findIndex((room) => room.roomId === roomId);
    const before = this.rooms[roomIndex];
    const isRemoval = change.action === LOBBY_ACTION.REMOVE || change.action === LOBBY_ACTION.DESTROY;

    if (isRemoval) {
      if (roomIndex === -1) {
        return;
      }
      this.rooms.splice(roomIndex, 1);
    } else if (!data) {
      return;
    } else if (roomIndex === -1) {
      this.rooms.push(data);
    } else {
      this.rooms[roomIndex] = data;
    }

    const entry: LobbyEventHistoryEntry<Metadata> = {
      seq: ++this.globalEventSeq,
      roomId,
      action: change.action,
      reason: change.reason,
      before,
      after: isRemoval ? undefined : data,
    };

    this.eventHistory.push(entry);
    while (this.eventHistory.length > this.eventHistorySize) {
      this.eventHistory.shift();
    }

    for (const client of this.clients) {
      const ids = this.subscriptionKeys.get(client.sessionId);

      if (!ids || ids.size === 0) {
        this.sendLegacyEvent(client, entry, this.resolveEvent(entry, this.clientOptions[client.sessionId]?.filter));
        continue;
      }

      this.sendLegacyEvent(client, entry, this.resolveEvent(entry, this.clientOptions[client.sessionId]?.filter));

      for (const id of ids) {
        const subscription = this.subscriptions.get(this.getSubscriptionKey(client.sessionId, id));
        if (subscription?.active) {
          this.dispatchSubscriptionEvent(client, subscription, entry);
        }
      }
    }
  }

  protected dispatchSubscriptionEvent(
    client: LobbyClient,
    subscription: LobbySubscriptionState,
    event: LobbyEventHistoryEntry<Metadata>,
  ) {
    const projected = this.resolveEvent(event, subscription.filter);

    if (!projected) {
      return;
    }

    const bufferedAmount = (client.ref as unknown as { bufferedAmount?: number }).bufferedAmount ?? 0;
    if (bufferedAmount >= this.maxBufferedBytes) {
      this.sendSnapshot(client, subscription, 'backpressure');
      this.sendStatus(client, subscription, LOBBY_SUBSCRIPTION_STATUS.ACTIVE);
      return;
    }

    subscription.seq++;
    subscription.globalSeq = event.seq;
    this.sendSubscriptionEvent(client, subscription, event, projected);
  }

  protected sendLegacyEvent(
    client: LobbyClient,
    event: LobbyEventHistoryEntry<Metadata>,
    projected: ResolvedLobbyEvent | undefined,
  ) {
    if (!projected) { return; }

    if (projected.action === 'destroy' || projected.action === 'remove') {
      client.send('-', event.roomId);
    } else if (event.after) {
      client.send('+', [event.roomId, event.after]);
    }
  }

  protected sendSubscriptionEvent(
    client: LobbyClient,
    subscription: LobbySubscriptionState,
    event: LobbyEventHistoryEntry<Metadata>,
    projected: ResolvedLobbyEvent,
  ) {
    client.send('lobby:event', {
      id: subscription.id,
      seq: subscription.seq,
      cursor: this.formatCursor(event.seq),
      action: projected.action,
      reason: projected.reason,
      roomId: event.roomId,
      room: event.after,
    } satisfies LobbySubscriptionEvent<Metadata>);
  }

  protected projectEvent(
    event: LobbyEventHistoryEntry<Metadata>,
    filter?: FilterInput,
  ): ResolvedLobbyEvent | undefined {
    return this.resolveEvent(event, filter);
  }

  protected resolveEvent(
    event: LobbyEventHistoryEntry<Metadata>,
    filter?: FilterInput,
  ): ResolvedLobbyEvent | undefined {
    const beforeMatched = event.before !== undefined && this.filterItemForClient(event.before, filter);
    const afterMatched = event.after !== undefined && this.filterItemForClient(event.after, filter);

    if (event.action === LOBBY_ACTION.DESTROY) {
      return beforeMatched ? { action: 'destroy', reason: 'destroyed' } : undefined;
    }

    if (event.action === LOBBY_ACTION.REMOVE) {
      return beforeMatched ? { action: 'remove', reason: 'private' } : undefined;
    }

    if (!afterMatched) {
      return beforeMatched ? { action: 'remove', reason: 'private' } : undefined;
    }

    if (!beforeMatched) {
      return { action: 'create' };
    }

    if (event.action === LOBBY_ACTION.LOCK || event.action === LOBBY_ACTION.UNLOCK) {
      return { action: event.after.locked ? 'lock' : 'unlock' };
    }

    if (event.before?.locked !== event.after?.locked) {
      return { action: event.after.locked ? 'lock' : 'unlock' };
    }

    return { action: 'update' };
  }

  protected removeClientSubscriptions(sessionId: string) {
    delete this.clientOptions[sessionId];

    const ids = this.subscriptionKeys.get(sessionId);
    if (ids) {
      for (const id of ids) {
        this.subscriptions.delete(this.getSubscriptionKey(sessionId, id));
      }
      this.subscriptionKeys.delete(sessionId);
    }
  }
}
