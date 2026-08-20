package io.akka.deepstream.domain;

/**
 * One user's live connection count — SPEC-001 §2, §3 rules 21 to 23.
 *
 * <p>A refcount rather than a flag: a user with two tabs open is present once, and closing one of
 * them announces nothing.
 */
public record PresenceState(String userId, int connections, long seq) {

  /** The user id deepstream gives a client that logged in without authenticating (rule 20). */
  public static final String ANONYMOUS = "OPEN";

  public static PresenceState empty(String userId) {
    return new PresenceState(userId, 0, 0);
  }

  public boolean online() {
    return connections > 0;
  }

  public PresenceState onEvent(PresenceEvent event) {
    return switch (event) {
      case PresenceEvent.Joined joined -> new PresenceState(userId, 1, joined.seq());
      case PresenceEvent.Left left -> new PresenceState(userId, 0, left.seq());
      case PresenceEvent.ConnectionOpened ignored ->
          new PresenceState(userId, connections + 1, seq);
      case PresenceEvent.ConnectionClosed ignored ->
          new PresenceState(userId, connections - 1, seq);
    };
  }
}
