package io.akka.deepstream.application;

import akka.Done;
import akka.javasdk.annotations.Component;
import akka.javasdk.keyvalueentity.KeyValueEntity;
import io.akka.deepstream.domain.RecordUpdate;

/**
 * One durable announcement, addressed by record name and position — SPEC-001 §3 rules 18, 19.
 *
 * <p>The id is {@code <name>@<seq>}, which is derived from the event that caused it, so the same
 * event delivered twice rewrites the same row rather than making a second announcement.
 */
@Component(id = "record-update")
public class RecordUpdateEntity extends KeyValueEntity<RecordUpdate> {

  public static String idFor(String recordName, long seq) {
    return recordName + "@" + seq;
  }

  public Effect<Done> put(RecordUpdate update) {
    return effects().updateState(update).thenReply(Done.getInstance());
  }
}
