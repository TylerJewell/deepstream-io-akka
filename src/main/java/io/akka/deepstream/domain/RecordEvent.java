package io.akka.deepstream.domain;

import akka.javasdk.annotations.TypeName;
import java.util.Map;

/**
 * What happened to a record.
 *
 * <p>Only accepted writes are events; a rejected write persists nothing (rule 7), which is what
 * makes the subscriber stream carry accepted writes and nothing else (rules 16, 17).
 */
public sealed interface RecordEvent {

  /**
   * Position in this record's own stream of announcements. Unlike the version it never resets, so
   * a subscriber can name where it got to even across a delete-and-recreate (rules 18, 19).
   */
  long seq();

  @TypeName("written")
  record Written(long seq, long version, Map<String, Object> data) implements RecordEvent {}

  /** Rule 26: a deleted record answers a version enquiry the way a missing one does. */
  @TypeName("deleted")
  record Deleted(long seq) implements RecordEvent {}
}
