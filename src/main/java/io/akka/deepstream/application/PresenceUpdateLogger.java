package io.akka.deepstream.application;

import akka.javasdk.annotations.Component;
import akka.javasdk.annotations.Consume;
import akka.javasdk.client.ComponentClient;
import akka.javasdk.consumer.Consumer;
import io.akka.deepstream.domain.PresenceEvent;
import io.akka.deepstream.domain.PresenceUpdate;

/**
 * Turns a join or a leave into one durable, addressable announcement — SPEC-001 §3 rules 18, 21,
 * 22.
 *
 * <p>The two silent events — a second connection opening, one of several closing — are recorded by
 * the entity so its count survives a restart, and announce nothing here.
 */
@Component(id = "presence-update-logger")
@Consume.FromEventSourcedEntity(PresenceEntity.class)
public class PresenceUpdateLogger extends Consumer {

  private final ComponentClient componentClient;

  public PresenceUpdateLogger(ComponentClient componentClient) {
    this.componentClient = componentClient;
  }

  public Effect onEvent(PresenceEvent event) {
    String userId = messageContext().eventSubject().orElseThrow();
    PresenceUpdate update =
        switch (event) {
          case PresenceEvent.Joined joined -> new PresenceUpdate(userId, joined.seq(), true);
          case PresenceEvent.Left left -> new PresenceUpdate(userId, left.seq(), false);
          case PresenceEvent.ConnectionOpened ignored -> null;
          case PresenceEvent.ConnectionClosed ignored -> null;
        };
    if (update == null) {
      return effects().ignore();
    }
    componentClient
        .forKeyValueEntity(PresenceUpdateEntity.idFor(userId, update.seq()))
        .method(PresenceUpdateEntity::put)
        .invoke(update);
    return effects().done();
  }
}
