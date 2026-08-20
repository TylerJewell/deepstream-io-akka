package io.akka.deepstream.domain;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A record, and the whole write decision — SPEC-001 §2 and §3 rules 1 to 15.
 *
 * <p>{@link #judge} is a pure function of the record and the request, so the rules it carries can
 * be checked without a runtime. The entity around it only persists what this returns.
 *
 * <p>A deleted record and a record that never existed are the same thing here (rule 26): both sit
 * at version 0 with no data, and a version enquiry answers {@code -1} for both.
 */
public record RecordState(String name, long seq, long version, Map<String, Object> data) {

  public static final long MISSING = -1L;

  /**
   * Rule 28. The target replicates a record's state across regions only while it stays under a
   * megabyte; past that the record becomes isolated where it was written. The ceiling is set below
   * that so the event carrying the record, and its envelope, fit inside the same budget.
   */
  public static final long MAX_RECORD_BYTES = 512 * 1024;

  /** What a write decided, and — when it was accepted — the data it produces. */
  public record Decision(WriteResult result, Map<String, Object> data) {}

  public static RecordState empty(String name) {
    return new RecordState(name, 0, 0, Map.of());
  }

  public boolean exists() {
    return version > 0;
  }

  /** Rule 26: what a version enquiry answers. */
  public long headVersion() {
    return exists() ? version : MISSING;
  }

  /** Rules 1 to 15. The data is present only when the result is {@link WriteResult.Accepted}. */
  public Decision judge(WriteRequest request) {
    if (!exists() && !request.upsert()) {
      return new Decision(new WriteResult.NotFound(), null);
    }

    long claimed = request.version();
    if (claimed != MISSING) {
      if (claimed > version + 1) {
        return new Decision(new WriteResult.InvalidVersion(version), null);
      }
      if (claimed <= version) {
        return new Decision(new WriteResult.VersionExists(version, data), null);
      }
    }

    Map<String, Object> next;
    try {
      next = apply(request);
    } catch (IllegalArgumentException e) {
      return new Decision(new WriteResult.InvalidPath(e.getMessage()), null);
    }
    long size = JsonPath.approximateJsonSize(next);
    if (size > MAX_RECORD_BYTES) {
      return new Decision(
          new WriteResult.TooLarge(MAX_RECORD_BYTES, size), null);
    }
    return new Decision(new WriteResult.Accepted(version + 1), next);
  }

  /**
   * The data a successful write produces. A batch (rule 12) can only be judged by trying it, so
   * this throws rather than returning a partial record, and the caller keeps the old one.
   */
  private Map<String, Object> apply(WriteRequest request) {
    return switch (request) {
      case WriteRequest.Update update -> {
        if (update.data() == null) {
          throw new IllegalArgumentException("an update names no data");
        }
        if (!JsonPath.hasOnlySafeKeys(update.data())) {
          throw new IllegalArgumentException(
              "a top-level key may not be __proto__, constructor or prototype");
        }
        yield new LinkedHashMap<>(update.data());
      }
      case WriteRequest.Patch patch -> JsonPath.set(data, patch.path(), patch.data());
      case WriteRequest.Erase erase -> JsonPath.erase(data, erase.path());
      case WriteRequest.PatchMulti batch -> {
        List<PatchOp> ops = batch.ops();
        if (ops == null || ops.isEmpty()) {
          throw new IllegalArgumentException("a batch names no operations");
        }
        yield JsonPath.setAll(data, ops);
      }
    };
  }

  public RecordState onEvent(RecordEvent event) {
    return switch (event) {
      case RecordEvent.Written written ->
          new RecordState(name, written.seq(), written.version(), written.data());
      case RecordEvent.Deleted deleted -> new RecordState(name, deleted.seq(), 0, Map.of());
    };
  }
}
