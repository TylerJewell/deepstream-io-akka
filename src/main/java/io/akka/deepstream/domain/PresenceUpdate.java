package io.akka.deepstream.domain;

/**
 * One announcement about one user — SPEC-001 §3 rules 18, 21, 22.
 *
 * <p>Only a join or a leave produces one; a second connection opening does not (rule 21).
 */
public record PresenceUpdate(String userId, long seq, boolean online) {}
