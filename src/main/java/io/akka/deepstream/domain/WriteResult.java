package io.akka.deepstream.domain;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.Map;

/**
 * What the record decided about a write — SPEC-001 §3 rules 1 to 8.
 *
 * <p>Five answers, and the difference between two of them is the point of the slice:
 * {@link VersionExists} means somebody else got there first and carries what won, so the caller
 * can rebase; {@link InvalidVersion} means the caller named a version that was never reachable
 * and carries no data, because there is nothing to rebase onto.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "outcome")
@JsonSubTypes({
  @JsonSubTypes.Type(value = WriteResult.Accepted.class, name = "accepted"),
  @JsonSubTypes.Type(value = WriteResult.VersionExists.class, name = "version_exists"),
  @JsonSubTypes.Type(value = WriteResult.InvalidVersion.class, name = "invalid_version"),
  @JsonSubTypes.Type(value = WriteResult.NotFound.class, name = "not_found"),
  @JsonSubTypes.Type(value = WriteResult.InvalidPath.class, name = "invalid_path"),
  @JsonSubTypes.Type(value = WriteResult.TooLarge.class, name = "too_large")
})
public sealed interface WriteResult {

  /** Rule 1: the write became this version. Unlike the source, an acceptance answers (OD-2). */
  record Accepted(long version) implements WriteResult {}

  /** Rules 3, 4: superseded, with the version and data that won. */
  record VersionExists(long version, Map<String, Object> data) implements WriteResult {}

  /** Rule 5: more than one past the current version. Carries the current version only. */
  record InvalidVersion(long currentVersion) implements WriteResult {}

  /** Rule 6: no such record, and the write did not ask to create one. */
  record NotFound() implements WriteResult {}

  /** Rules 12 to 15: the write named something it may not name. */
  record InvalidPath(String reason) implements WriteResult {}

  /** Rule 28: the record the write would produce is past the size the target will replicate. */
  record TooLarge(long limitBytes, long wouldBeBytes) implements WriteResult {}
}
