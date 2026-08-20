package io.akka.deepstream.domain;

import akka.javasdk.annotations.TypeName;

/**
 * What happened to a user's connections — SPEC-001 §3 rules 21, 22.
 *
 * <p>Four events, of which only two announce anything. A second connection opening, or one of
 * several closing, moves the refcount and is recorded so the count survives a restart, but it is
 * not a join or a leave and no subscriber hears about it.
 *
 * <p>{@code seq} counts announcements for this user and only ever grows, so a subscriber that
 * dropped can say where it got to (rule 18). The two silent events leave it where it was.
 */
public sealed interface PresenceEvent {

  @TypeName("joined")
  record Joined(long seq) implements PresenceEvent {}

  @TypeName("left")
  record Left(long seq) implements PresenceEvent {}

  @TypeName("connection-opened")
  record ConnectionOpened() implements PresenceEvent {}

  @TypeName("connection-closed")
  record ConnectionClosed() implements PresenceEvent {}
}
