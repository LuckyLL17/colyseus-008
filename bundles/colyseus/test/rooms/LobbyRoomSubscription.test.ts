import assert from "assert";
import { matchMaker, LobbyRoom, Server, Room, type MatchMakerDriver, type Presence } from "@colyseus/core";
import * as Colyseus from "@colyseus/sdk";
import { DummyRoom, DRIVERS, PRESENCE_IMPLEMENTATIONS, timeout } from "./../utils/index.ts";

describe("LobbyRoom: cursor subscriptions", () => {
  for (let i = 0; i < PRESENCE_IMPLEMENTATIONS.length; i++) {
    for (let j = 0; j < DRIVERS.length; j++) {
      let presence: Presence;
      let driver: MatchMakerDriver;
      let server: Server;
      const TEST_PORT = 4100 + Math.floor((Math.random() * 1000));
      const TEST_ENDPOINT = `ws://localhost:${TEST_PORT}`;

      describe(`Driver => ${DRIVERS[j].name}, Presence => ${PRESENCE_IMPLEMENTATIONS[i].name}`, () => {
        class LobbyAwareRoom extends Room {
          async onCreate(options: any) {
            if (options.roomId) { this.roomId = options.roomId; }
            if (options.metadata) { await this.setMetadata(options.metadata); }

            this.onMessage("metadata", (_client, metadata: any) => this.setMetadata(metadata));
            this.onMessage("private", (_client, value: boolean) => this.setPrivate(value));
          }
        }

        beforeEach(async () => {
          presence = new PRESENCE_IMPLEMENTATIONS[i]();
          driver = new DRIVERS[j]();
          server = new Server({
            greet: false,
            gracefullyShutdown: false,
            presence,
            driver,
          });

          matchMaker.setup(presence, driver);
          matchMaker.defineRoomType("lobby", LobbyRoom);
          matchMaker.defineRoomType("dummy_1", LobbyAwareRoom).enableRealtimeListing();
          matchMaker.defineRoomType("dummy_2", DummyRoom).enableRealtimeListing();

          if (driver.boot) { await driver.boot(); }
          await driver.clear();
          await server.listen(TEST_PORT);
        });

        afterEach(async () => {
          await server.gracefullyShutdown(false);
          await driver.shutdown();
        });

        it("creates a continuous subscription with snapshot boundary, create, update, lock and destroy", async () => {
          const client = new Colyseus.Client(TEST_ENDPOINT);
          const lobby = await client.join("lobby");
          const subscription = new Colyseus.LobbySubscription(lobby, {
            name: "dummy_1",
            metadata: { region: "eu" },
          });
          const events: Colyseus.LobbySubscriptionEvent[] = [];
          let snapshot!: Colyseus.LobbySnapshot;

          subscription.onSnapshot((value) => { snapshot = value; });
          subscription.onEvent((event) => events.push(event));

          await timeout(10);
          assert.strictEqual(subscription.status, "active");
          assert.deepStrictEqual(snapshot.rooms, []);
          assert.strictEqual(snapshot.seq, 0);

          const room = await matchMaker.createRoom("dummy_1", {});
          await matchMaker.remoteRoomCall(room.roomId, "metadata", [{ region: "eu", level: 1 }]);
          await timeout(50);

          assert.strictEqual(events.length, 1);
          assert.strictEqual(events[0]!.action, "create");
          assert.strictEqual(events[0]!.seq, 1);
          assert.strictEqual(events[0]!.room!.metadata.region, "eu");
          assert.strictEqual(subscription.rooms.size, 1);

          await matchMaker.remoteRoomCall(room.roomId, "metadata", [{ region: "eu", level: 2 }]);
          await timeout(50);
          assert.strictEqual(events.length, 2);
          assert.strictEqual(events[1]!.action, "update");
          assert.strictEqual(events[1]!.seq, 2);

          await matchMaker.remoteRoomCall(room.roomId, "lock");
          await timeout(50);
          assert.strictEqual(events.length, 3);
          assert.strictEqual(events[2]!.action, "lock");
          assert.strictEqual(events[2]!.seq, 3);

          await matchMaker.remoteRoomCall(room.roomId, "disconnect");
          await timeout(50);
          assert.strictEqual(events.length, 4);
          assert.strictEqual(events[3]!.action, "destroy");
          assert.strictEqual(events[3]!.reason, "destroyed");
          assert.strictEqual(events[3]!.seq, 4);
          assert.strictEqual(subscription.rooms.size, 0);
        });

        it("replays a valid cursor and returns a snapshot boundary when the cursor is invalid", async () => {
          const serverLobby = await matchMaker.createRoom("lobby", { eventHistorySize: 100, maxBufferedEvents: 2 });
          const lobbyRoom = await matchMaker.getLocalRoomById(serverLobby.roomId) as LobbyRoom;
          const created = await matchMaker.createRoom("dummy_1", {});
          await matchMaker.remoteRoomCall(created.roomId, "metadata", [{ region: "eu" }]);
          await timeout(20);

          const firstCursor = lobbyRoom.globalEventSeq;
          assert.ok(firstCursor >= 2);

          await matchMaker.createRoom("dummy_1", {});
          await timeout(20);

          const client = new Colyseus.Client(TEST_ENDPOINT);
          const lobby = await client.join("lobby");

          const validEvents: Colyseus.LobbySubscriptionEvent[] = [];
          const valid = new Colyseus.LobbySubscription(lobby, {
            id: "valid",
            name: "dummy_1",
            cursor: `${lobbyRoom.epoch}:${firstCursor}`,
          });
          valid.onEvent((event) => validEvents.push(event));
          await timeout(20);
          assert.strictEqual(validEvents.length, 1);
          assert.strictEqual(validEvents[0]!.action, "create");
          assert.strictEqual(validEvents[0]!.seq, 1);

          const invalidSnapshots: Colyseus.LobbySnapshot[] = [];
          const invalid = new Colyseus.LobbySubscription(lobby, {
            id: "invalid",
            name: "dummy_1",
            cursor: `${lobbyRoom.epoch}:${lobbyRoom.globalEventSeq - 100}`,
          });
          invalid.onSnapshot((snapshot) => invalidSnapshots.push(snapshot));
          await timeout(20);
          assert.strictEqual(invalidSnapshots.length, 1);
          assert.strictEqual(invalidSnapshots[0].reason, "invalid-cursor");
          assert.strictEqual(invalidSnapshots[0].rooms.length, 2);
          assert.strictEqual(invalid.rooms.size, 2);
          assert.strictEqual(invalid.status, "active");
        });

        it("uses a full snapshot instead of replaying an excessive backlog", async () => {
          const serverLobby = await matchMaker.createRoom("lobby", { eventHistorySize: 100, maxBufferedEvents: 1 });
          const lobbyRoom = await matchMaker.getLocalRoomById(serverLobby.roomId) as LobbyRoom;
          await matchMaker.createRoom("dummy_1", {});
          await timeout(20);
          await matchMaker.createRoom("dummy_1", {});
          await matchMaker.createRoom("dummy_1", {});
          await timeout(30);

          const client = new Colyseus.Client(TEST_ENDPOINT);
          const lobby = await client.join("lobby");
          const snapshots: Colyseus.LobbySnapshot[] = [];
          const events: Colyseus.LobbySubscriptionEvent[] = [];
          const subscription = new Colyseus.LobbySubscription(lobby, {
            name: "dummy_1",
            cursor: `${lobbyRoom.epoch}:1`,
          });
          subscription.onSnapshot((snapshot) => snapshots.push(snapshot));
          subscription.onEvent((event) => events.push(event));

          await timeout(20);
          assert.strictEqual(events.length, 0);
          assert.strictEqual(snapshots.length, 1);
          assert.strictEqual(snapshots[0].reason, "backlog");
          assert.strictEqual(snapshots[0].rooms.length, 3);
        });

        it("returns a full snapshot when the cursor history has rolled away", async () => {
          const serverLobby = await matchMaker.createRoom("lobby", { maxBufferedEvents: 2 });
          const lobbyRoom = await matchMaker.getLocalRoomById(serverLobby.roomId) as LobbyRoom;
          await matchMaker.createRoom("dummy_1", {});
          await matchMaker.createRoom("dummy_1", {});
          await matchMaker.createRoom("dummy_1", {});
          await timeout(30);

          const client = new Colyseus.Client(TEST_ENDPOINT);
          const lobby = await client.join("lobby");
          const snapshots: Colyseus.LobbySnapshot[] = [];
          const events: Colyseus.LobbySubscriptionEvent[] = [];
          const subscription = new Colyseus.LobbySubscription(lobby, {
            name: "dummy_1",
            cursor: `${lobbyRoom.epoch}:0`,
          });
          subscription.onSnapshot((snapshot) => snapshots.push(snapshot));
          subscription.onEvent((event) => events.push(event));

          await timeout(20);
          assert.strictEqual(events.length, 0);
          assert.strictEqual(snapshots.length, 1);
          assert.strictEqual(snapshots[0].reason, "invalid-cursor");
          assert.strictEqual(snapshots[0].rooms.length, 3);
        });

        it("resynchronizes a subscription after same-room reconnection", async () => {
          const serverLobby = await matchMaker.createRoom("lobby", { reconnectionTime: 2 });
          const lobbyRoom = await matchMaker.getLocalRoomById(serverLobby.roomId) as LobbyRoom;

          const client = new Colyseus.Client(TEST_ENDPOINT);
          const lobby = await client.join("lobby");
          lobby.reconnection.minUptime = 0;
          lobby.reconnection.minDelay = 0;
          lobby.reconnection.delay = 0;
          lobby.reconnection.backoff = () => 0;
          lobby.reconnection.maxRetries = 3;

          const statuses: string[] = [];
          const events: Colyseus.LobbySubscriptionEvent[] = [];
          const subscription = new Colyseus.LobbySubscription(lobby, { name: "dummy_1" });
          subscription.onStatusChange((status) => statuses.push(status));
          subscription.onEvent((event) => events.push(event));

          await matchMaker.createRoom("dummy_1", {});
          await timeout(50);
          assert.strictEqual(events.length, 1);
          lobby.connection.close(1006);
          await timeout(200);

          assert.deepStrictEqual(statuses, ["syncing", "active", "resyncing", "active"]);
          assert.strictEqual(subscription.status, "active");

          await matchMaker.createRoom("dummy_1", {});
          await timeout(50);
          assert.strictEqual(events.length, 2);
          assert.strictEqual(events[1]!.seq, 2);
          assert.strictEqual(events[1]!.cursor, `${lobbyRoom.epoch}:2`);
          assert.strictEqual(subscription.rooms.size, 2);
          assert.strictEqual(lobbyRoom.rooms.length, 2);
        });

        it("ends a subscription with an explicit status", async () => {
          const client = new Colyseus.Client(TEST_ENDPOINT);
          const lobby = await client.join("lobby");
          const subscription = new Colyseus.LobbySubscription(lobby, { id: "ending" });
          const endedReasons: string[] = [];
          subscription.onEnd((reason) => endedReasons.push(reason ?? ""));

          await timeout(20);
          subscription.unsubscribe();
          await timeout(20);

          assert.strictEqual(subscription.status, "ended");
          assert.deepStrictEqual(endedReasons, ["unsubscribed"]);
        });

        it("marks private rooms as a filtered removal without calling it destruction", async () => {
          const client = new Colyseus.Client(TEST_ENDPOINT);
          const room = await client.create("dummy_1", {});
          const lobby = await client.join("lobby");
          const events: Colyseus.LobbySubscriptionEvent[] = [];
          const subscription = new Colyseus.LobbySubscription(lobby, { name: "dummy_1" });
          subscription.onEvent((event) => events.push(event));

          await timeout(50);
          assert.strictEqual(events.length, 1);

          room.send("private", true);
          await timeout(50);
          assert.strictEqual(events.length, 2);
          assert.strictEqual(events[1].action, "remove");
          assert.strictEqual(events[1].reason, "private");
          assert.strictEqual(subscription.rooms.size, 0);
        });

        it("still serves the legacy rooms, plus and minus messages", async () => {
          const client = new Colyseus.Client(TEST_ENDPOINT);
          const lobby = await client.join("lobby");
          let roomsLength: number | undefined;
          const added: string[] = [];
          const removed: string[] = [];

          lobby.onMessage("rooms", (rooms) => { roomsLength = rooms.length; });
          lobby.onMessage("+", ([roomId]) => added.push(roomId));
          lobby.onMessage("-", (roomId) => removed.push(roomId));

          const room = await client.create("dummy_2", {});
          await timeout(50);

          await room.leave(true);
          await timeout(50);

          assert.strictEqual(roomsLength, 0);
          assert.strictEqual(added.length, 1);
          assert.strictEqual(removed.length, 1);
        });
      });
    }
  }
});
