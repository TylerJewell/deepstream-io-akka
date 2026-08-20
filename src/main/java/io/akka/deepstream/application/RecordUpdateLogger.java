package io.akka.deepstream.application;

import akka.javasdk.annotations.Component;
import akka.javasdk.annotations.Consume;
import akka.javasdk.client.ComponentClient;
import akka.javasdk.consumer.Consumer;
import io.akka.deepstream.domain.RecordEvent;
import akka.javasdk.JsonSupport;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.akka.deepstream.domain.RecordUpdate;

/**
 * Turns every accepted write into one durable, addressable announcement — SPEC-001 §3 rules 16
 * to 19.
 *
 * <p>Only accepted writes reach here: a rejected write persists no event (rule 7), so rule 17
 * holds without this having to decide anything.
 */
@Component(id = "record-update-logger")
@Consume.FromEventSourcedEntity(RecordEntity.class)
public class RecordUpdateLogger extends Consumer {

  private final ComponentClient componentClient;

  public RecordUpdateLogger(ComponentClient componentClient) {
    this.componentClient = componentClient;
  }

  public Effect onEvent(RecordEvent event) {
    String name = messageContext().eventSubject().orElseThrow();
    RecordUpdate update =
        switch (event) {
          case RecordEvent.Written written ->
              new RecordUpdate(
                  name, written.seq(), written.version(), asJson(written.data()), false);
          case RecordEvent.Deleted deleted ->
              new RecordUpdate(name, deleted.seq(), -1, "{}", true);
        };
    componentClient
        .forKeyValueEntity(RecordUpdateEntity.idFor(name, update.seq()))
        .method(RecordUpdateEntity::put)
        .invoke(update);
    return effects().done();
  }

  private static String asJson(java.util.Map<String, Object> data) {
    try {
      return JsonSupport.getObjectMapper().writeValueAsString(data);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("a record that was persisted cannot be written as JSON", e);
    }
  }
}
