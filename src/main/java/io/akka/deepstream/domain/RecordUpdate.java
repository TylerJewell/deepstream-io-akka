package io.akka.deepstream.domain;

/**
 * One announcement about one record — SPEC-001 §3 rules 16 to 19.
 *
 * <p>One of these exists per accepted write, keyed by the record's name and the position in its
 * stream, so a subscriber that dropped can ask for what came after the last position it saw
 * instead of being handed the current state and left to guess what it missed.
 *
 * <p>Each announcement carries the whole record as it stood at that version rather than the path
 * that was written, so a subscriber applies it without holding the versions before it.
 *
 * <p>The record's contents travel as JSON text: this type is a view row, and a view row may not
 * hold a map.
 */
public record RecordUpdate(
    String name, long seq, long version, String dataJson, boolean deleted) {}
