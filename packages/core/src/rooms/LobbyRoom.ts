import * as matchMaker from '../MatchMaker.ts';
import type { IRoomCache } from '../matchmaker/LocalDriver/LocalDriver.ts';
import type { Client } from '../Transport.ts';
import { subscribeLobby } from '../matchmaker/Lobby.ts';
import { Room } from '../Room.ts';

// TODO: use Schema state & filters on version 1.0.0

// class DummyLobbyState extends Schema { // tslint:disable-line
//   @type("number") public _: number;
// }

export interface FilterInput {
  name?: string;
  metadata?: any;
}

export interface LobbyOptions {
  filter?: FilterInput;
}

/**
 * Type of change carried by a {@link LobbyRoomEvent}.
 */
export type LobbyRoomEventType = 'create' | 'update' | 'lock' | 'unlock' | 'destroy';

/**
 * A single entry of the lobby's event journal. Events are delivered to
 * subscribed clients in batches (see {@link LobbyEventsMessage}).
 */
export interface LobbyRoomEvent<Metadata = any> {
  /**
   * Position of this event in the lobby's journal. Strictly increasing —
   * clients use it as their resumption cursor.
   */
  seq: number;

  type: LobbyRoomEventType;

  roomId: string;

  /**
   * Room listing after the change. On 'destroy' events this is the last
   * known listing of the removed room (kept for filter matching).
   */
  room: IRoomCache<Metadata>;
}

/** Why the server sent a full snapshot instead of incremental events. */
export type LobbySnapshotReason =
  | 'initial'   // fresh subscription, no cursor provided
  | 'resync'    // provided cursor was expired or invalid
  | 'overflow'; // client backlog grew past `maxClientBacklog`

/** Why a subscription ended. */
export type LobbyEndReason = 'unsubscribe' | 'disposed';

/** Payload of the client-sent "subscribe" message. */
export interface LobbySubscribeOptions {
  filter?: FilterInput;
  cursor?: number;
}

/**
 * Server → client: full listing boundary. Replaces the client's entire view
 * and re-baselines its cursor to `seq`.
 */
export interface LobbySnapshotMessage<Metadata = any> {
  seq: number;
  rooms: Array<IRoomCache<Metadata>>;
  reason: LobbySnapshotReason;
}

/**
 * Server → client: batch of incremental journal events.
 */
export interface LobbyEventsMessage<Metadata = any> {
  /** Cursor the batch builds upon — must match the client's current cursor. */
  since: number;
  /** Journal position after applying this batch — the client's next cursor. */
  upto: number;
  events: Array<LobbyRoomEvent<Metadata>>;
}

/** Server → client: the subscription has ended. */
export interface LobbyEndedMessage {
  reason: LobbyEndReason;
}

/**
 * Server-side view of a client's subscription state.
 * - `'live'` — receiving incremental events.
 * - `'resync'` — backlog overflowed; a full snapshot is scheduled for the next flush.
 * - `'ended'` — terminated (unsubscribed, left, or room disposed).
 */
export type LobbySubscriptionState = 'live' | 'resync' | 'ended';

export interface LobbyClientSubscription {
  filter?: FilterInput;
  /** Journal position the client has confirmed applied (last `upto` sent). */
  cursor: number;
  /** Events queued for the next flush (backlog for slow clients). */
  pending: Array<LobbyRoomEvent>;
  state: LobbySubscriptionState;
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
    snapshot: LobbySnapshotMessage;
    events: LobbyEventsMessage;
    ended: LobbyEndedMessage;
  }
}>;

export type LobbyMessages = {
  filter: (client: LobbyClient, filter: FilterInput) => void;
  subscribe: (client: LobbyClient, options?: LobbySubscribeOptions) => void;
  unsubscribe: (client: LobbyClient) => void;
};

export class LobbyRoom<Metadata = any> extends Room {
  public rooms: IRoomCache<Metadata>[] = [];
  public unsubscribeLobby: () => void;

  public clientOptions: { [sessionId: string]: LobbyOptions } = {};

  /**
   * Maximum number of journal entries retained for cursor replay. Clients
   * resuming with a cursor older than the oldest retained entry receive a
   * full snapshot (`reason: 'resync'`) instead of incremental events.
   */
  public eventLogSize: number = 500;

  /**
   * Maximum number of events queued per subscription between flushes. When a
   * (slow) client exceeds this backlog, its queued events are discarded and a
   * full snapshot (`reason: 'overflow'`) is sent on the next flush, instead of
   * a patch chain the client cannot apply.
   */
  public maxClientBacklog: number = 100;

  /**
   * Batching window (in milliseconds) for delivering subscription events.
   * Set to `0` to flush every event synchronously.
   */
  public eventFlushInterval: number = 50;

  /**
   * Active incremental subscriptions, keyed by sessionId.
   */
  public subscriptions: { [sessionId: string]: LobbyClientSubscription } = {};

  /**
   * Journal of room-listing changes, oldest first. Entries are evicted (in
   * FIFO order) once the journal grows beyond `eventLogSize`.
   */
  protected eventLog: Array<LobbyRoomEvent<Metadata>> = [];
  protected lastSeq: number = 0;

  /** Last known locked flag per roomId (drives lock/unlock event detection). */
  private lockedByRoomId: Map<string, boolean> = new Map();

  private flushTimer?: ReturnType<typeof setTimeout>;

  declare messages: LobbyMessages;

  public async onCreate(options: any) {
    // prevent LobbyRoom to notify itself
    this['_listing'].unlisted = true;

    this.onMessage('filter', (client, filter) => {
      const clientOptions = this.clientOptions[client.sessionId];
      if (!clientOptions) { return; }

      clientOptions.filter = filter;
      client.send('rooms', this.filterItemsForClient(clientOptions));
    });

    this.onMessage('subscribe', (client, options) => this.handleSubscribe(client, options ?? {}));
    this.onMessage('unsubscribe', (client) => this.handleUnsubscribe(client));

    this.unsubscribeLobby = await subscribeLobby((roomId, data) => {
      const roomIndex = this.rooms.findIndex((room) => room.roomId === roomId);
      const clients = this.clients.filter((client) => this.clientOptions[client.sessionId]);

      if (!data) {
        // remove room listing data
        if (roomIndex !== -1) {
          const previousData = this.rooms[roomIndex];

          this.rooms.splice(roomIndex, 1);
          this.lockedByRoomId.delete(roomId);

          clients.forEach((client) => {
            if (this.filterItemForClient(previousData, this.clientOptions[client.sessionId].filter)) {
              client.send('-', roomId);
            }
          });

          this.appendEvent({ type: 'destroy', roomId, room: previousData });
        }

      } else if (roomIndex === -1) {
        // append room listing data
        this.rooms.push(data);
        this.lockedByRoomId.set(roomId, data.locked ?? false);

        clients.forEach((client) => {
          if (this.filterItemForClient(data, this.clientOptions[client.sessionId].filter)) {
            client.send('+', [roomId, data]);
          }
        });

        this.appendEvent({ type: 'create', roomId, room: data });

      } else {
        const previousData = this.rooms[roomIndex];

        // replace room listing data
        this.rooms[roomIndex] = data;

        clients.forEach((client) => {
          const hadData = this.filterItemForClient(previousData, this.clientOptions[client.sessionId].filter);
          const hasData = this.filterItemForClient(data, this.clientOptions[client.sessionId].filter);

          if (hadData && !hasData) {
            client.send('-', roomId);

          } else if (hasData) {
            client.send('+', [roomId, data]);
          }
        });

        // (drivers may mutate the cached listing in place, so the locked flag
        // is diffed against our own bookkeeping rather than `previousData`)
        const wasLocked = this.lockedByRoomId.get(roomId) ?? false;
        const isLocked = data.locked ?? false;
        this.lockedByRoomId.set(roomId, isLocked);

        this.appendEvent({
          type: (wasLocked !== isLocked)
            ? ((isLocked) ? 'lock' : 'unlock')
            : 'update',
          roomId,
          room: data,
        });
      }
    });

    this.rooms = await matchMaker.query({ private: false, unlisted: false });

    for (const room of this.rooms) {
      this.lockedByRoomId.set(room.roomId, room.locked ?? false);
    }
  }

  public onJoin(client: LobbyClient, options: LobbyOptions) {
    this.clientOptions[client.sessionId] = (
      !Array.isArray(options) && // Defold (Lua) sends empty objects as Array instead of object
      options
    ) || {};
    client.send('rooms', this.filterItemsForClient(this.clientOptions[client.sessionId]));
  }

  public onLeave(client: LobbyClient) {
    const subscription = this.subscriptions[client.sessionId];
    if (subscription) {
      subscription.state = 'ended';
      delete this.subscriptions[client.sessionId];
    }
    delete this.clientOptions[client.sessionId];
  }

  public disconnect(closeCode?: number): Promise<any> {
    // notify subscribers before their connections are closed (clients are
    // still joined here — messages can no longer be sent from onLeave/onDispose)
    for (const sessionId in this.subscriptions) {
      this.subscriptions[sessionId].state = 'ended';
      this.clients.get(sessionId)?.send('ended', { reason: 'disposed' });
    }
    this.subscriptions = {};

    return super.disconnect(closeCode);
  }

  public onDispose() {
    if (this.flushTimer !== undefined) {
      clearTimeout(this.flushTimer);
      this.flushTimer = undefined;
    }

    if (this.unsubscribeLobby) {
      this.unsubscribeLobby();
    }
  }

  /**
   * Handle a "subscribe" message: start (or replace) the client's incremental
   * subscription. Depending on the provided cursor, the client receives:
   *
   * - no cursor → "snapshot" (`reason: 'initial'`)
   * - a cursor still covered by the journal → "events" replaying what it missed
   * - an expired/invalid cursor → "snapshot" (`reason: 'resync'`) — a full
   *   boundary, never a patch chain the client cannot apply.
   */
  protected handleSubscribe(client: LobbyClient, options: LobbySubscribeOptions) {
    const filter = options?.filter;
    const cursor = (typeof (options?.cursor) === 'number' && isFinite(options.cursor) && options.cursor > 0)
      ? Math.floor(options.cursor)
      : undefined;

    // oldest journal position still available for replay
    const firstSeq = this.lastSeq - this.eventLog.length + 1;

    this.subscriptions[client.sessionId] = {
      filter,
      cursor: this.lastSeq,
      pending: [],
      state: 'live',
    };

    if (cursor === undefined) {
      client.send('snapshot', {
        seq: this.lastSeq,
        rooms: this.filterItemsForClient({ filter }),
        reason: 'initial',
      });

    } else if (cursor <= this.lastSeq && cursor >= firstSeq - 1) {
      // cursor still covered by the journal — replay what was missed
      client.send('events', {
        since: cursor,
        upto: this.lastSeq,
        events: this.eventLog.filter((event) =>
          event.seq > cursor && this.filterItemForClient(event.room, filter)),
      });

    } else {
      // cursor expired (evicted from the journal) or invalid
      client.send('snapshot', {
        seq: this.lastSeq,
        rooms: this.filterItemsForClient({ filter }),
        reason: 'resync',
      });
    }
  }

  protected handleUnsubscribe(client: LobbyClient) {
    const subscription = this.subscriptions[client.sessionId];
    if (!subscription) { return; }

    subscription.state = 'ended';
    delete this.subscriptions[client.sessionId];
    client.send('ended', { reason: 'unsubscribe' });
  }

  /**
   * Append an event to the journal and queue it on every matching
   * subscription. Delivery happens on the next flush (see `eventFlushInterval`).
   */
  protected appendEvent(event: Omit<LobbyRoomEvent<Metadata>, 'seq'>) {
    const entry: LobbyRoomEvent<Metadata> = { ...event, seq: ++this.lastSeq };

    this.eventLog.push(entry);
    if (this.eventLog.length > this.eventLogSize) {
      this.eventLog.splice(0, this.eventLog.length - this.eventLogSize);
    }

    for (const sessionId in this.subscriptions) {
      const subscription = this.subscriptions[sessionId];
      if (subscription.state !== 'live') { continue; }
      if (!this.filterItemForClient(entry.room, subscription.filter)) { continue; }

      subscription.pending.push(entry);

      if (subscription.pending.length > this.maxClientBacklog) {
        // slow client: dropping single events would leave it with a patch
        // chain it cannot apply — discard the backlog and resync with a full
        // snapshot boundary on the next flush instead.
        subscription.pending.length = 0;
        subscription.state = 'resync';
      }
    }

    this.scheduleFlush();
  }

  private scheduleFlush() {
    if (this.eventFlushInterval <= 0) {
      this.flushSubscriptions();
      return;
    }

    if (this.flushTimer !== undefined) { return; }

    this.flushTimer = setTimeout(() => {
      this.flushTimer = undefined;
      this.flushSubscriptions();
    }, this.eventFlushInterval);
  }

  protected flushSubscriptions() {
    for (const sessionId in this.subscriptions) {
      const subscription = this.subscriptions[sessionId];
      const client = this.clients.get(sessionId) as LobbyClient;
      if (!client) { continue; }

      if (subscription.state === 'resync') {
        subscription.state = 'live';
        subscription.cursor = this.lastSeq;
        client.send('snapshot', {
          seq: this.lastSeq,
          rooms: this.filterItemsForClient({ filter: subscription.filter }),
          reason: 'overflow',
        });

      } else if (subscription.pending.length > 0) {
        const events = subscription.pending;
        subscription.pending = [];

        client.send('events', { since: subscription.cursor, upto: this.lastSeq, events });
        subscription.cursor = this.lastSeq;
      }
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

    let isAllowed = true;

    if (filter.name !== room.name) {
      isAllowed = false;
    }

    if (filter.metadata) {
      for (const field in filter.metadata) {
        if (room.metadata?.[field] !== filter.metadata[field]) {
          isAllowed = false;
          break;
        }
      }
    }

    return isAllowed;
  }

}
