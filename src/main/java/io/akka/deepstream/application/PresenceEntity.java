package io.akka.deepstream.application;

import akka.javasdk.annotations.Component;
import akka.javasdk.eventsourcedentity.EventSourcedEntity;
import akka.javasdk.eventsourcedentity.EventSourcedEntityContext;
import io.akka.deepstream.domain.PresenceEvent;
import io.akka.deepstream.domain.PresenceState;

/**
 * One user's presence — SPEC-001 §3 rules 21 to 23.
 *
 * <p>The source keeps this count per node and pushes only the zero-to-one and one-to-zero
 * transitions into a registry shared across the cluster. Here the user is the entity, so the count
 * is already one thing wherever the connections landed.
 *
 * <p>Both commands reply with whether they announced something, which is what a caller needs to
 * know and what the refcount exists to decide.
 */
@Component(id = "presence")
public class PresenceEntity extends EventSourcedEntity<PresenceState, PresenceEvent> {

  private final String userId;

  public PresenceEntity(EventSourcedEntityContext context) {
    this.userId = context.entityId();
  }

  @Override
  public PresenceState emptyState() {
    return PresenceState.empty(userId);
  }

  /** Rule 21: the first connection announces a join; later ones are counted and silent. */
  public Effect<Boolean> connect() {
    if (currentState().online()) {
      return effects().persist(new PresenceEvent.ConnectionOpened()).thenReply(state -> false);
    }
    return effects()
        .persist(new PresenceEvent.Joined(currentState().seq() + 1))
        .thenReply(state -> true);
  }

  /** Rules 22, 23: the last connection announces a leave; a disconnect with none does nothing. */
  public Effect<Boolean> disconnect() {
    var state = currentState();
    if (state.connections() == 0) {
      return effects().reply(false);
    }
    if (state.connections() > 1) {
      return effects().persist(new PresenceEvent.ConnectionClosed()).thenReply(s -> false);
    }
    return effects().persist(new PresenceEvent.Left(state.seq() + 1)).thenReply(s -> true);
  }

  @Override
  public PresenceState applyEvent(PresenceEvent event) {
    return currentState().onEvent(event);
  }
}
