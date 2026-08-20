package io.akka.deepstream.application;

import akka.javasdk.annotations.Component;
import akka.javasdk.eventsourcedentity.EventSourcedEntity;
import akka.javasdk.eventsourcedentity.EventSourcedEntityContext;
import io.akka.deepstream.domain.RecordEvent;
import io.akka.deepstream.domain.RecordState;
import io.akka.deepstream.domain.WriteRequest;
import io.akka.deepstream.domain.WriteResult;

/**
 * One record, and the only place a write is decided — SPEC-001 §3 rules 1 to 15, 26, 27.
 *
 * <p>The source spreads this across a handler that queues writes, a transition object that lives
 * only as long as the queue, and two stores neither of which is waited for. Here a record is one
 * entity, so writes to it are already serialised and the version check needs no queue: two writers
 * claiming the same version reach this handler one after the other, and the second one sees the
 * first one's version.
 */
@Component(id = "record")
public class RecordEntity extends EventSourcedEntity<RecordState, RecordEvent> {

  private final String name;

  public RecordEntity(EventSourcedEntityContext context) {
    this.name = context.entityId();
  }

  public record Snapshot(String name, long version, java.util.Map<String, Object> data) {}

  @Override
  public RecordState emptyState() {
    return RecordState.empty(name);
  }

  /** Rules 1 to 15. Every write answers, including a successful one (OD-2). */
  public Effect<WriteResult> write(WriteRequest request) {
    var decision = currentState().judge(request);
    if (!(decision.result() instanceof WriteResult.Accepted accepted)) {
      return effects().reply(decision.result());
    }
    return effects()
        .persist(
            new RecordEvent.Written(
                currentState().seq() + 1, accepted.version(), decision.data()))
        .thenReply(state -> decision.result());
  }

  /** Rule 26. A record that never existed and one that was deleted answer the same. */
  public ReadOnlyEffect<Long> head() {
    return effects().reply(currentState().headVersion());
  }

  public ReadOnlyEffect<Snapshot> read() {
    var state = currentState();
    return effects().reply(new Snapshot(state.name(), state.headVersion(), state.data()));
  }

  /** Rule 26. Deleting a record that is not there changes nothing. */
  public Effect<Long> delete() {
    if (!currentState().exists()) {
      return effects().reply(RecordState.MISSING);
    }
    return effects()
        .persist(new RecordEvent.Deleted(currentState().seq() + 1))
        .thenReply(state -> RecordState.MISSING);
  }

  @Override
  public RecordState applyEvent(RecordEvent event) {
    return currentState().onEvent(event);
  }
}
