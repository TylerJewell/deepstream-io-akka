package io.akka.deepstream.domain;

/** One operation inside a batch write — SPEC-001 §3 rule 12. */
public record PatchOp(String path, Object data) {}
