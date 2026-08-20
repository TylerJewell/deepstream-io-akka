package io.akka.deepstream.application;

import akka.Done;
import akka.javasdk.annotations.Component;
import akka.javasdk.keyvalueentity.KeyValueEntity;
import io.akka.deepstream.domain.PresenceUpdate;

/**
 * One durable presence announcement, addressed by user and position — SPEC-001 §3 rules 18, 19.
 *
 * <p>The id is {@code <userId>@<seq>}, derived from the event that caused it, so a redelivered
 * event rewrites the same row rather than announcing twice.
 */
@Component(id = "presence-update")
public class PresenceUpdateEntity extends KeyValueEntity<PresenceUpdate> {

  public static String idFor(String userId, long seq) {
    return userId + "@" + seq;
  }

  public Effect<Done> put(PresenceUpdate update) {
    return effects().updateState(update).thenReply(Done.getInstance());
  }
}
