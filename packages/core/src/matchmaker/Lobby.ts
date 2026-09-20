import * as matchMaker from '../MatchMaker.ts';

import type { Room } from '../Room.ts';
import type { IRoomCache } from './driver.ts';

const LOBBY_CHANNEL = '$lobby';

export const LOBBY_ACTION = {
  UPSERT: '0',
  REMOVE: '1',
  CREATE: '2',
  LOCK: '3',
  UNLOCK: '4',
  DESTROY: '5',
} as const;

export type LobbyAction = typeof LOBBY_ACTION[keyof typeof LOBBY_ACTION];

export interface LobbyChange {
  action: LobbyAction;
  reason?: 'private' | 'destroyed';
}

/*
 * TODO: refactor this on 1.0
 *
 * Some users might be relying on "1" = "removed" from the lobby due to this workaround: https://github.com/colyseus/colyseus/issues/617
 * Though, for consistency, we should invert as "0" = "invisible" and "1" = "visible".
 *
 * - rename "removed" to "isVisible" and swap the logic
 * - emit "visibility-change" with inverted value (isVisible)
 * - update "subscribeLobby" to check "1" as "isVisible" instead of "removed"
 */

export function updateLobby<T extends Room>(
  room: T,
  removed: boolean | LobbyAction = false,
  action?: LobbyAction,
) {
  const listing = room['_listing'];

  if (listing.unlisted || !listing.roomId) {
    return;
  }

  if (removed === true || removed === LOBBY_ACTION.REMOVE || removed === LOBBY_ACTION.DESTROY) {
    const wireAction = removed === LOBBY_ACTION.DESTROY ? LOBBY_ACTION.DESTROY : LOBBY_ACTION.REMOVE;
    matchMaker.presence.publish(LOBBY_CHANNEL, `${listing.roomId},${wireAction}`);
  } else if (!listing.private) {
    matchMaker.presence.publish(LOBBY_CHANNEL, `${listing.roomId},${action ?? LOBBY_ACTION.UPSERT}`);
  }
}

export async function subscribeLobby(
  callback: (roomId: string, roomListing: IRoomCache | null, change: LobbyChange) => void,
) {
  // Track removed roomIds to prevent race conditions where pending queries
  // complete after a room has been removed
  const removedRoomIds = new Set<string>();

  const cb = async (message: string) => {
    const [roomId, wireAction] = message.split(',');
    const action: LobbyAction = (wireAction === LOBBY_ACTION.CREATE ||
      wireAction === LOBBY_ACTION.LOCK ||
      wireAction === LOBBY_ACTION.UNLOCK ||
      wireAction === LOBBY_ACTION.REMOVE ||
      wireAction === LOBBY_ACTION.DESTROY)
      ? wireAction
      : LOBBY_ACTION.UPSERT;

    if (action === LOBBY_ACTION.REMOVE || action === LOBBY_ACTION.DESTROY) {
      // Mark as removed and process immediately
      removedRoomIds.add(roomId);
      callback(
        roomId,
        null,
        { action, reason: action === LOBBY_ACTION.DESTROY ? 'destroyed' : 'private' },
      );

      // Clean up after a short timeout to prevent memory leaks
      setTimeout(() => removedRoomIds.delete(roomId), 2000);

    } else {
      // Clear removed status - room might be coming back (e.g., visibility change)
      removedRoomIds.delete(roomId);

      const room = (await matchMaker.query({ roomId }))[0];

      // Check if room was removed while we were querying
      // See "updating metadata should not cause race condition" test in LobbyRoom.test.ts
      if (removedRoomIds.has(roomId)) {
        return;
      }

      callback(roomId, room, { action });
    }
  };

  await matchMaker.presence.subscribe(LOBBY_CHANNEL, cb);

  return () => matchMaker.presence.unsubscribe(LOBBY_CHANNEL, cb);
}
