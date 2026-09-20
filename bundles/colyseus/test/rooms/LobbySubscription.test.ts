import assert from "assert";
import * as Colyseus from "@colyseus/sdk";
import { LobbySubscription, type LobbyRoomEvent, type LobbySubscriptionState } from "@colyseus/sdk";
import { matchMaker, Server, LobbyRoom, type MatchMakerDriver } from "@colyseus/core";
import { DummyRoom, DRIVERS, timeout, PRESENCE_IMPLEMENTATIONS } from "./../utils/index.ts";

type WireMessage = [type: string, payload: any];

function messagesOfType(received: WireMessage[], type: string) {
  return received.filter(([t]) => t === type).map(([, payload]) => payload);
}

function flattenEvents(received: WireMessage[]): LobbyRoomEvent[] {
  const batches = messagesOfType(received, 'events');
  // batches must chain: each batch starts where the previous one ended
  for (let i = 1; i < batches.length; i++) {
    assert.strictEqual(batches[i].since, batches[i - 1].upto, "event batches must chain (since === previous upto)");
  }
  return batches.flatMap((batch) => batch.events);
}

function assertContinuousSeqs(events: LobbyRoomEvent[]) {
  for (let i = 1; i < events.length; i++) {
    assert.strictEqual(events[i].seq, events[i - 1].seq + 1, "event seqs must be continuous");
  }
}

describe("LobbyRoom: Subscription", () => {
  for (let i = 0; i < PRESENCE_IMPLEMENTATIONS.length; i++) {
    const presence = new PRESENCE_IMPLEMENTATIONS[i]();

    for (let j = 0; j < DRIVERS.length; j++) {
      let driver: MatchMakerDriver = new DRIVERS[j]();

      describe(`Driver => ${(driver.constructor as any).name}, Presence => ${presence.constructor.name}`, () => {
        const TEST_PORT = 4600 + Math.floor((Math.random() * 300));
        const TEST_ENDPOINT = `ws://localhost:${TEST_PORT}`;

        const server = new Server({
          greet: false,
          presence,
          driver
        });

        const client = new Colyseus.Client(TEST_ENDPOINT);

        async function joinLobby() {
          const lobby = await client.joinOrCreate("lobby");
          const serverLobby = matchMaker.getLocalRoomById(lobby.roomId) as LobbyRoom;
          serverLobby.eventFlushInterval = 10; // speed up tests
          return { lobby, serverLobby };
        }

        async function createDummy(name: string = 'dummy_1') {
          const roomData = await matchMaker.createRoom(name, {});
          const room = matchMaker.getLocalRoomById(roomData.roomId);
          room.autoDispose = false;
          return roomData;
        }

        before(async () => {
          await server.listen(TEST_PORT);
        });

        beforeEach(async () => {
          // setup matchmaker
          await matchMaker.setup(presence, driver);
          await matchMaker.accept();
          // boot the driver to ensure table exists
          if (driver.boot) { await driver.boot(); }
          await driver.clear();

          // define a room
          matchMaker.defineRoomType("lobby", LobbyRoom);
          matchMaker.defineRoomType("dummy_1", DummyRoom).enableRealtimeListing();
          matchMaker.defineRoomType("dummy_2", DummyRoom).enableRealtimeListing();
        });

        after(async () => {
          await server.gracefullyShutdown(false);
          await driver.shutdown();
        });
        afterEach(async () => await matchMaker.gracefullyShutdown());

        describe("wire protocol", () => {

          it("subscribe receives initial snapshot, then incremental events", async () => {
            const { lobby } = await joinLobby();

            const received: WireMessage[] = [];
            lobby.onMessage('snapshot', (m) => received.push(['snapshot', m]));
            lobby.onMessage('events', (m) => received.push(['events', m]));

            // legacy full-list message must still be delivered on join
            let legacyRoomsReceived = false;
            lobby.onMessage('rooms', () => legacyRoomsReceived = true);

            lobby.send('subscribe', {});
            await timeout(100);

            assert.strictEqual(received.length, 1);
            const snapshot = messagesOfType(received, 'snapshot')[0];
            assert.strictEqual(snapshot.reason, 'initial');
            assert.strictEqual(snapshot.seq, 0);
            assert.deepStrictEqual(snapshot.rooms, []);
            assert.ok(legacyRoomsReceived, "legacy 'rooms' message should still be sent");

            // create two rooms → two journal events
            const room1 = await createDummy('dummy_1');
            const room2 = await createDummy('dummy_2');
            await timeout(150);

            const events = flattenEvents(received);
            assert.strictEqual(events.length, 2);
            assert.deepStrictEqual(events.map((e) => e.type), ['create', 'create']);
            assert.deepStrictEqual(events.map((e) => e.roomId), [room1.roomId, room2.roomId]);
            assert.strictEqual(events[0].room!.name, 'dummy_1');
            assertContinuousSeqs(events);

            // batch metadata: chain starts at the snapshot's seq
            const batches = messagesOfType(received, 'events');
            assert.strictEqual(batches[0].since, 0);
            assert.strictEqual(batches[batches.length - 1].upto, 2);

            await lobby.leave();
          });

          it("emits update, lock, unlock and destroy events", async () => {
            const { lobby } = await joinLobby();

            const received: WireMessage[] = [];
            lobby.onMessage('snapshot', (m) => received.push(['snapshot', m]));
            lobby.onMessage('events', (m) => received.push(['events', m]));

            lobby.send('subscribe', {});
            await timeout(50);

            const roomData = await createDummy('dummy_1');
            const room = matchMaker.getLocalRoomById(roomData.roomId);
            await timeout(100);

            await room.lock();
            await timeout(100);

            await room.unlock();
            await timeout(100);

            await room.setMetadata({ mode: 'ranked' });
            await timeout(100);

            await matchMaker.remoteRoomCall(roomData.roomId, "disconnect");
            await timeout(100);

            const events = flattenEvents(received);
            assert.deepStrictEqual(events.map((e) => e.type), ['create', 'lock', 'unlock', 'update', 'destroy']);
            assertContinuousSeqs(events);

            // destroy carries the last known listing
            const destroy = events[events.length - 1];
            assert.strictEqual(destroy.roomId, roomData.roomId);
            assert.strictEqual(destroy.room!.name, 'dummy_1');

            await lobby.leave();
          });

          it("filters events by room name and metadata", async () => {
            const { lobby } = await joinLobby();

            const received: WireMessage[] = [];
            lobby.onMessage('snapshot', (m) => received.push(['snapshot', m]));
            lobby.onMessage('events', (m) => received.push(['events', m]));

            lobby.send('subscribe', { filter: { name: 'dummy_1', metadata: { mode: 'ranked' } } });
            await timeout(50);

            // does not match: wrong name
            await createDummy('dummy_2');
            // does not match: no metadata yet
            const roomData = await createDummy('dummy_1');
            const room = matchMaker.getLocalRoomById(roomData.roomId);
            await timeout(100);

            let events = flattenEvents(received);
            assert.strictEqual(events.length, 0, "non-matching rooms must not be delivered");

            // now it matches
            await room.setMetadata({ mode: 'ranked' });
            await timeout(100);

            events = flattenEvents(received);
            assert.strictEqual(events.length, 1);
            assert.strictEqual(events[0].type, 'update');
            assert.strictEqual(events[0].roomId, roomData.roomId);

            // no longer matches → not delivered
            await room.setMetadata({ mode: 'casual' });
            await timeout(100);

            events = flattenEvents(received);
            assert.strictEqual(events.length, 1, "rooms leaving the filter must not be delivered as events");

            // destroy of a matching room is delivered (with last known data)
            await room.setMetadata({ mode: 'ranked' });
            await timeout(100);
            await matchMaker.remoteRoomCall(roomData.roomId, "disconnect");
            await timeout(100);

            events = flattenEvents(received);
            assert.strictEqual(events[events.length - 1].type, 'destroy');
            assert.strictEqual(events[events.length - 1].roomId, roomData.roomId);

            await lobby.leave();
          });

          it("resumes from a valid cursor by replaying missed events", async () => {
            const { lobby, serverLobby } = await joinLobby();

            const received: WireMessage[] = [];
            lobby.onMessage('snapshot', (m) => received.push(['snapshot', m]));
            lobby.onMessage('events', (m) => received.push(['events', m]));

            lobby.send('subscribe', {});
            await timeout(50);

            const room1 = await createDummy('dummy_1');
            await timeout(100);
            const cursor = flattenEvents(received).at(-1)!.seq;

            // miss some events while "disconnected"
            const room2 = await createDummy('dummy_1');
            const room3 = await createDummy('dummy_2');
            await timeout(100);

            // resume from the cursor → replay, no new snapshot
            received.length = 0;
            lobby.send('subscribe', { cursor });
            await timeout(100);

            assert.strictEqual(messagesOfType(received, 'snapshot').length, 0, "valid cursor must not trigger a snapshot");
            const replayed = flattenEvents(received);
            assert.deepStrictEqual(replayed.map((e) => e.roomId), [room2.roomId, room3.roomId]);
            assert.strictEqual(replayed[0].seq, cursor + 1);

            // up-to-date cursor → empty replay
            received.length = 0;
            lobby.send('subscribe', { cursor: (serverLobby as any).lastSeq });
            await timeout(100);
            assert.strictEqual(messagesOfType(received, 'snapshot').length, 0);
            assert.strictEqual(flattenEvents(received).length, 0);

            await lobby.leave();
          });

          it("returns a full snapshot boundary when the cursor expired", async () => {
            const { lobby, serverLobby } = await joinLobby();
            serverLobby.eventLogSize = 2;

            const received: WireMessage[] = [];
            lobby.onMessage('snapshot', (m) => received.push(['snapshot', m]));
            lobby.onMessage('events', (m) => received.push(['events', m]));

            lobby.send('subscribe', {});
            await timeout(50);

            await createDummy('dummy_1');
            await timeout(100);
            const staleCursor = flattenEvents(received).at(-1)!.seq;

            // evict the journal past the client's cursor (eventLogSize = 2)
            await createDummy('dummy_1');
            await createDummy('dummy_2');
            await createDummy('dummy_2');
            await timeout(100);

            received.length = 0;
            lobby.send('subscribe', { cursor: staleCursor });
            await timeout(100);

            const snapshots = messagesOfType(received, 'snapshot');
            assert.strictEqual(snapshots.length, 1, "expired cursor must answer with a snapshot boundary");
            assert.strictEqual(snapshots[0].reason, 'resync');
            assert.strictEqual(snapshots[0].rooms.length, 4, "snapshot carries the full listing");
            assert.strictEqual(flattenEvents(received).length, 0, "no unapplicable patches are sent");

            await lobby.leave();
          });

          it("resyncs slow clients with a snapshot when the backlog overflows", async () => {
            const { lobby, serverLobby } = await joinLobby();
            serverLobby.maxClientBacklog = 2;
            serverLobby.eventFlushInterval = 300; // long window so events pile up

            const received: WireMessage[] = [];
            lobby.onMessage('snapshot', (m) => received.push(['snapshot', m]));
            lobby.onMessage('events', (m) => received.push(['events', m]));

            lobby.send('subscribe', {});
            await timeout(50);

            // burst of events beyond the backlog cap
            await createDummy('dummy_1');
            await createDummy('dummy_1');
            await createDummy('dummy_2');
            await createDummy('dummy_2');

            // subscription is marked for resync on the server
            await timeout(100);
            const subscription = serverLobby.subscriptions[lobby.sessionId];
            assert.strictEqual(subscription.state, 'resync', "overflowing subscription is marked 'resync'");

            await timeout(400);

            const snapshots = messagesOfType(received, 'snapshot');
            assert.strictEqual(snapshots.length, 2, "initial + overflow snapshot");
            assert.strictEqual(snapshots[1].reason, 'overflow');
            assert.strictEqual(snapshots[1].rooms.length, 4);
            assert.strictEqual(flattenEvents(received).length, 0, "dropped events are never sent as patches");
            assert.strictEqual(subscription.state, 'live', "subscription is live again after the resync");

            // still receives events afterwards
            await createDummy('dummy_1');
            await timeout(400);
            assert.strictEqual(flattenEvents(received).length, 1);

            await lobby.leave();
          });

          it("ends the subscription on unsubscribe", async () => {
            const { lobby, serverLobby } = await joinLobby();

            const received: WireMessage[] = [];
            lobby.onMessage('snapshot', (m) => received.push(['snapshot', m]));
            lobby.onMessage('events', (m) => received.push(['events', m]));
            lobby.onMessage('ended', (m) => received.push(['ended', m]));

            lobby.send('subscribe', {});
            await timeout(50);
            assert.strictEqual(serverLobby.subscriptions[lobby.sessionId]?.state, 'live');

            lobby.send('unsubscribe');
            await timeout(50);

            const ended = messagesOfType(received, 'ended');
            assert.strictEqual(ended.length, 1);
            assert.strictEqual(ended[0].reason, 'unsubscribe');
            assert.strictEqual(serverLobby.subscriptions[lobby.sessionId], undefined, "subscription is removed");

            // no further events are delivered
            await createDummy('dummy_1');
            await timeout(100);
            assert.strictEqual(flattenEvents(received).length, 0);

            await lobby.leave();
          });

          it("notifies subscribers when the lobby is disposed", async () => {
            const { lobby } = await joinLobby();

            const received: WireMessage[] = [];
            lobby.onMessage('snapshot', () => { });
            lobby.onMessage('ended', (m) => received.push(['ended', m]));

            lobby.send('subscribe', {});
            await timeout(50);

            await matchMaker.remoteRoomCall(lobby.roomId, "disconnect");
            await timeout(100);

            const ended = messagesOfType(received, 'ended');
            assert.strictEqual(ended.length, 1);
            assert.strictEqual(ended[0].reason, 'disposed');
          });

        });

        describe("SDK LobbySubscription", () => {

          it("syncs an initial snapshot and applies incremental events", async () => {
            const { lobby } = await joinLobby();

            const states: LobbySubscriptionState[] = [];
            const events: LobbyRoomEvent[] = [];
            let snapshotRooms: any[] | undefined;

            const sub = new LobbySubscription(lobby);
            sub.onStateChange((state) => states.push(state));
            sub.onEvent((event) => events.push(event));
            sub.onSnapshot((rooms) => snapshotRooms = rooms);

            assert.strictEqual(sub.state, 'syncing');
            await timeout(100);

            assert.strictEqual(sub.state, 'live');
            assert.strictEqual(sub.rooms.size, 0);
            assert.deepStrictEqual(snapshotRooms, []);

            const roomData = await createDummy('dummy_1');
            await timeout(150);

            assert.strictEqual(sub.rooms.size, 1);
            assert.strictEqual(sub.get(roomData.roomId)?.name, 'dummy_1');
            assert.strictEqual(events.length, 1);
            assert.strictEqual(events[0].type, 'create');
            assert.strictEqual(sub.cursor, events[0].seq);

            // destroy is applied to the materialized view
            await matchMaker.remoteRoomCall(roomData.roomId, "disconnect");
            await timeout(150);
            assert.strictEqual(sub.rooms.size, 0);
            assert.strictEqual(events[1].type, 'destroy');

            await lobby.leave();
          });

          it("resyncs after re-joining through attach()", async () => {
            const { lobby, serverLobby } = await joinLobby();
            serverLobby.autoDispose = false; // keep the lobby (and its journal) alive

            const states: LobbySubscriptionState[] = [];
            const sub = new LobbySubscription(lobby);
            sub.onStateChange((state) => states.push(state));
            await timeout(100);

            const room1 = await createDummy('dummy_1');
            await timeout(150);
            assert.strictEqual(sub.rooms.size, 1);

            // "disconnect": leave the lobby; events keep flowing server-side
            await lobby.leave();
            assert.strictEqual(sub.state, 'ended');

            const room2 = await createDummy('dummy_2');
            await timeout(100);

            // re-join and resume from the preserved cursor
            let resynced: any[] | undefined;
            sub.onResync((rooms) => resynced = rooms);

            const lobby2 = await client.joinOrCreate("lobby");
            sub.attach(lobby2);
            assert.strictEqual(sub.state, 'resyncing');
            await timeout(150);

            assert.strictEqual(sub.state, 'live');
            assert.strictEqual(resynced, undefined, "cursor was still valid — no full snapshot needed");
            assert.strictEqual(sub.rooms.size, 2, "missed events are replayed on top of the preserved view");
            assert.ok(sub.rooms.has(room1.roomId));
            assert.ok(sub.rooms.has(room2.roomId));
            assert.ok(states.includes('resyncing'));

            await lobby2.leave();
          });

          it("falls back to a full snapshot when the cursor expired", async () => {
            const { lobby, serverLobby } = await joinLobby();
            serverLobby.autoDispose = false;
            serverLobby.eventLogSize = 2;

            const sub = new LobbySubscription(lobby);
            await timeout(100);

            await createDummy('dummy_1');
            await timeout(150);
            assert.strictEqual(sub.rooms.size, 1);

            await lobby.leave();

            // evict the journal past the subscription's cursor
            await createDummy('dummy_1');
            await createDummy('dummy_2');
            await createDummy('dummy_2');
            await timeout(100);

            let resynced: any[] | undefined;
            let resyncReason: string | undefined;
            sub.onResync((rooms, reason) => { resynced = rooms; resyncReason = reason; });

            const lobby2 = await client.joinOrCreate("lobby");
            sub.attach(lobby2);
            await timeout(150);

            assert.strictEqual(sub.state, 'live');
            assert.strictEqual(resyncReason, 'resync');
            assert.strictEqual(resynced!.length, 4, "resync snapshot carries the full listing");
            assert.strictEqual(sub.rooms.size, 4, "view is fully replaced by the snapshot boundary");

            await lobby2.leave();
          });

          it("recovers from a broken event chain by re-syncing", async () => {
            const { lobby } = await joinLobby();

            const sub = new LobbySubscription(lobby);
            await timeout(100);
            assert.strictEqual(sub.state, 'live');

            // simulate a lost batch: server sends a batch that does not chain
            (lobby as any).onMessageHandlers.emit('events', {
              since: sub.cursor + 5,
              upto: sub.cursor + 5,
              events: [],
            });
            await timeout(100);

            assert.strictEqual(sub.state, 'live', "subscription healed itself via replay");

            // and keeps receiving events afterwards
            const roomData = await createDummy('dummy_1');
            await timeout(150);
            assert.strictEqual(sub.rooms.size, 1);
            assert.ok(sub.rooms.has(roomData.roomId));

            await lobby.leave();
          });

          it("ends on unsubscribe()", async () => {
            const { lobby } = await joinLobby();

            let endedReason: string | undefined;
            const sub = new LobbySubscription(lobby);
            sub.onEnded((reason) => endedReason = reason);
            await timeout(100);

            sub.unsubscribe();
            assert.strictEqual(sub.state, 'ended');
            await timeout(50);
            assert.strictEqual(endedReason, 'unsubscribe');

            await createDummy('dummy_1');
            await timeout(150);
            assert.strictEqual(sub.rooms.size, 0, "no events are applied after unsubscribe");

            await lobby.leave();
          });

          it("ends when the lobby room is disposed", async () => {
            const { lobby } = await joinLobby();

            let endedReason: string | undefined;
            const sub = new LobbySubscription(lobby);
            sub.onEnded((reason) => endedReason = reason);
            await timeout(100);

            await matchMaker.remoteRoomCall(lobby.roomId, "disconnect");
            await timeout(150);

            assert.strictEqual(sub.state, 'ended');
            assert.strictEqual(endedReason, 'disposed');
          });

        });

      });

    }
  }
});
