import { createSignal } from './core/signal.ts';
import type { Room } from './Room.ts';

/**
 * Type of change carried by a {@link LobbyRoomEvent} — mirrors
 * `LobbyRoomEventType` from `@colyseus/core`.
 */
export type LobbyRoomEventType = 'create' | 'update' | 'lock' | 'unlock' | 'destroy';

/**
 * A room listing entry, as delivered through a lobby subscription.
 * (Wire shape of the server's `IRoomCache`.)
 */
export interface LobbyRoomInfo<Metadata = any> {
    name: string;
    roomId: string;
    clients: number;
    maxClients: number;
    locked?: boolean;
    private?: boolean;
    metadata?: Metadata;
    processId?: string;
    publicAddress?: string;
    createdAt?: string | Date;
}

/**
 * A single incremental change on the lobby's room listing.
 */
export interface LobbyRoomEvent<Metadata = any> {
    /** Position of this event in the lobby's journal. Strictly increasing. */
    seq: number;
    type: LobbyRoomEventType;
    roomId: string;
    /** Room listing after the change. On 'destroy', the last known listing. */
    room?: LobbyRoomInfo<Metadata>;
}

/**
 * Lifecycle of a {@link LobbySubscription}:
 * - `'syncing'` — subscribe sent, waiting for the first snapshot.
 * - `'live'` — caught up; applying incremental events.
 * - `'resyncing'` — re-synchronizing: resuming from a cursor after
 *   reconnect/attach, waiting for either an event replay or a full snapshot.
 * - `'ended'` — unsubscribed, or the underlying room was left.
 */
export type LobbySubscriptionState = 'syncing' | 'live' | 'resyncing' | 'ended';

/**
 * Subscription filter. Rooms match by `name` (room type) and — when
 * `metadata` is provided — by exact equality on each listed metadata field.
 */
export interface LobbyFilter {
    name?: string;
    metadata?: { [field: string]: any };
}

export interface LobbySubscriptionOptions {
    /** Only rooms matching this filter are tracked and delivered. */
    filter?: LobbyFilter;
    /**
     * Resume position from a previous subscription. Only meaningful together
     * with the view state that cursor was taken from — prefer {@link LobbySubscription.attach}
     * for reconnections, which preserves both.
     */
    cursor?: number;
}

interface SnapshotMessage {
    seq: number;
    rooms: Array<LobbyRoomInfo<any>>;
    reason: 'initial' | 'resync' | 'overflow';
}

interface EventsMessage {
    since: number;
    upto: number;
    events: Array<LobbyRoomEvent<any>>;
}

/**
 * Cursor-based incremental subscription to a `LobbyRoom`.
 *
 * Wraps a room connection joined to the server's lobby and keeps a local,
 * always-consistent view of the room listing:
 *
 * - the server journals every create/update/lock/unlock/destroy with a
 *   continuous sequence number;
 * - this subscription tracks the journal position as its {@link cursor};
 * - after a disconnect, {@link attach} (or an automatic room reconnect)
 *   resumes from the cursor — the server replays what was missed, or sends a
 *   full snapshot boundary when the cursor expired;
 * - slow consumers are never sent a broken patch chain: the server falls
 *   back to a full snapshot (`onResync`) when the backlog overflows.
 *
 * @example
 * ```typescript
 * const lobby = await client.joinOrCreate("lobby");
 * const sub = new LobbySubscription(lobby, { filter: { name: "battle" } });
 *
 * sub.onSnapshot((rooms) => renderList(rooms));
 * sub.onEvent((event) => console.log(event.type, event.roomId));
 * sub.onResync((rooms, reason) => renderList(rooms)); // full reset boundary
 * sub.onStateChange((state) => console.log("subscription:", state));
 * ```
 */
export class LobbySubscription<Metadata = any> {
    /**
     * Materialized view of the room listing, keyed by roomId. Updated in
     * place as snapshots and events are applied.
     */
    public readonly rooms: Map<string, LobbyRoomInfo<Metadata>> = new Map();

    /** Fired when the initial full snapshot has been applied. */
    public onSnapshot = createSignal<(rooms: Array<LobbyRoomInfo<Metadata>>) => void>();

    /** Fired for each applied incremental event (create/update/lock/unlock/destroy). */
    public onEvent = createSignal<(event: LobbyRoomEvent<Metadata>) => void>();

    /**
     * Fired when the subscription had to fall back to a full snapshot instead
     * of incremental events (`reason` is `'resync'` for an expired cursor,
     * `'overflow'` for a backlog overflow). The previous view was discarded —
     * treat this as a full reset boundary.
     */
    public onResync = createSignal<(rooms: Array<LobbyRoomInfo<Metadata>>, reason: string) => void>();

    /** Fired whenever {@link state} changes. */
    public onStateChange = createSignal<(state: LobbySubscriptionState) => void>();

    /** Fired once the subscription has ended (unsubscribed / room left / disposed). */
    public onEnded = createSignal<(reason: string) => void>();

    protected room: Room;
    protected filter?: LobbyFilter;

    #state: LobbySubscriptionState = 'syncing';
    #cursor: number = 0;
    #unbindRoom?: () => void;

    constructor(room: Room, options: LobbySubscriptionOptions = {}) {
        this.filter = options.filter;
        this.#cursor = (typeof (options.cursor) === "number" && options.cursor > 0)
            ? Math.floor(options.cursor)
            : 0;

        this.bindRoom(room);
        this.sendSubscribe();
    }

    /** Current subscription state. */
    public get state(): LobbySubscriptionState {
        return this.#state;
    }

    /**
     * Journal position this subscription has applied so far. Pass it to a new
     * subscription (or keep it across {@link attach}) to resume incrementally.
     */
    public get cursor(): number {
        return this.#cursor;
    }

    /** Current room listing as an array (copy of the materialized view). */
    public get list(): Array<LobbyRoomInfo<Metadata>> {
        return Array.from(this.rooms.values());
    }

    /** Get a single room listing by roomId. */
    public get(roomId: string): LobbyRoomInfo<Metadata> | undefined {
        return this.rooms.get(roomId);
    }

    /**
     * Start a fresh subscription, optionally replacing the filter. Discards
     * the cursor — the server answers with a full snapshot.
     */
    public subscribe(filter?: LobbyFilter): void {
        if (filter !== undefined) {
            this.filter = filter;
        }
        this.#cursor = 0;
        this.sendSubscribe();
    }

    /**
     * Re-synchronize preserving the current cursor: the server replays the
     * events missed since {@link cursor}, or sends a full snapshot if the
     * cursor expired. Called automatically on room reconnection.
     */
    public resync(): void {
        this.sendSubscribe();
    }

    /**
     * Rebind this subscription to a new lobby room connection (e.g. after the
     * previous one was dropped) and resume from the current cursor. The
     * materialized view is preserved; missed events are replayed on top of it,
     * or — when the cursor expired — replaced by a full snapshot (`onResync`).
     */
    public attach(room: Room): void {
        this.unbindRoom();
        this.bindRoom(room);
        this.sendSubscribe();
    }

    /**
     * End the subscription. The materialized view is kept as-is; no further
     * events are applied.
     */
    public unsubscribe(): void {
        if (this.#state === 'ended') { return; }

        if (this.room?.connection?.isOpen) {
            this.room.send('unsubscribe');
        }
        this.setEnded('unsubscribe');
    }

    protected bindRoom(room: Room): void {
        this.room = room;

        const offSnapshot = room.onMessage('snapshot', (message: SnapshotMessage) => this.handleSnapshot(message));
        const offEvents = room.onMessage('events', (message: EventsMessage) => this.handleEvents(message));
        const offEnded = room.onMessage('ended', (message: { reason: string }) => this.setEnded(message?.reason ?? 'ended'));

        const onLeave = () => this.setEnded('leave');
        const onReconnect = () => this.resync();

        room.onLeave(onLeave);
        room.onReconnect(onReconnect);

        this.#unbindRoom = () => {
            offSnapshot();
            offEvents();
            offEnded();
            room.onLeave.remove(onLeave);
            room.onReconnect.remove(onReconnect);
        };
    }

    protected unbindRoom(): void {
        this.#unbindRoom?.();
        this.#unbindRoom = undefined;
    }

    protected sendSubscribe(): void {
        this.setState((this.#cursor > 0) ? 'resyncing' : 'syncing');

        const payload: { filter?: LobbyFilter, cursor?: number } = {};
        if (this.filter !== undefined) { payload.filter = this.filter; }
        if (this.#cursor > 0) { payload.cursor = this.#cursor; }

        this.room.send('subscribe', payload);
    }

    protected handleSnapshot(message: SnapshotMessage): void {
        if (this.#state === 'ended') { return; }

        this.rooms.clear();
        for (const room of message.rooms) {
            this.rooms.set(room.roomId, room);
        }
        this.#cursor = message.seq;

        this.setState('live');

        if (message.reason === 'initial') {
            this.onSnapshot.invoke(this.list);

        } else {
            // 'resync' | 'overflow' — the incremental chain was replaced by a
            // full boundary; the previous view no longer applies.
            this.onResync.invoke(this.list, message.reason);
        }
    }

    protected handleEvents(message: EventsMessage): void {
        if (this.#state === 'ended') { return; }

        if (message.since !== undefined && message.since > this.#cursor) {
            // broken chain — a batch was lost. Do not apply patches we cannot
            // chain: re-sync from our cursor (replay or snapshot boundary).
            this.resync();
            return;
        }

        const chained = (message.since === this.#cursor);

        for (const event of message.events) {
            // skip events already applied (overlapping replay / duplicate batch)
            if (event.seq <= this.#cursor) { continue; }

            if (event.type === 'destroy') {
                this.rooms.delete(event.roomId);

            } else if (event.room) {
                this.rooms.set(event.roomId, event.room);
            }

            this.onEvent.invoke(event);
        }

        if (message.upto > this.#cursor) {
            this.#cursor = message.upto;
        }

        if (chained || message.since === undefined) {
            this.setState('live');
        }
    }

    protected setState(state: LobbySubscriptionState): void {
        if (this.#state === state) { return; }
        this.#state = state;
        this.onStateChange.invoke(state);
    }

    protected setEnded(reason: string): void {
        if (this.#state === 'ended') { return; }
        this.#state = 'ended';
        this.onStateChange.invoke('ended');
        this.onEnded.invoke(reason);
    }

}
