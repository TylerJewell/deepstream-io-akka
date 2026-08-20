package io.akka.deepstream.domain;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.List;
import java.util.Map;

/**
 * A write a client asked for — SPEC-001 §2.
 *
 * <p>{@code version} is what the writer believes it is producing; {@code -1} means "whatever comes
 * next" (rule 2). {@code upsert} is the source's separate CREATEANDUPDATE action folded into a
 * flag, because it changes one branch of the decision (rule 6) and nothing else.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "action")
@JsonSubTypes({
  @JsonSubTypes.Type(value = WriteRequest.Update.class, name = "update"),
  @JsonSubTypes.Type(value = WriteRequest.Patch.class, name = "patch"),
  @JsonSubTypes.Type(value = WriteRequest.Erase.class, name = "erase"),
  @JsonSubTypes.Type(value = WriteRequest.PatchMulti.class, name = "patch_multi")
})
public sealed interface WriteRequest {

  long version();

  boolean upsert();

  /** Rule 9: replaces the record's data entirely. */
  record Update(long version, Map<String, Object> data, boolean upsert) implements WriteRequest {}

  /** Rule 10: writes one value at one path. */
  record Patch(long version, String path, Object data, boolean upsert) implements WriteRequest {}

  /** Rule 11: deletes the key at one path. */
  record Erase(long version, String path, boolean upsert) implements WriteRequest {}

  /** Rule 12: a batch, applied in order or not at all. */
  record PatchMulti(long version, List<PatchOp> ops, boolean upsert) implements WriteRequest {}
}
