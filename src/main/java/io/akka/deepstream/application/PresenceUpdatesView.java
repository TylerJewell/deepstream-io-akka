package io.akka.deepstream.application;

import akka.javasdk.annotations.Component;
import akka.javasdk.annotations.Consume;
import akka.javasdk.annotations.Query;
import akka.javasdk.view.TableUpdater;
import akka.javasdk.view.View;
import io.akka.deepstream.domain.PresenceUpdate;
import java.util.List;

/**
 * The subscriber stream for presence — SPEC-001 §3 rules 18, 21, 22.
 *
 * <p>Same shape as the record stream: an ordered replay of what a subscriber missed, then a
 * pushed stream of what follows. A client watching one user passes that user's id; a client
 * watching everybody passes none.
 */
@Component(id = "presence-updates")
public class PresenceUpdatesView extends View {

  @Consume.FromKeyValueEntity(PresenceUpdateEntity.class)
  public static class Updates extends TableUpdater<PresenceUpdate> {
    public Effect<PresenceUpdate> onUpdate(PresenceUpdate update) {
      return effects().updateRow(update);
    }
  }

  /** A view query takes at most one argument, so the two bounds travel together. */
  public record From(String userId, long sinceSeq) {}

  public record Announcements(List<PresenceUpdate> updates) {}

  @Query("SELECT * AS updates FROM updates WHERE seq > :sinceSeq ORDER BY seq")
  public QueryEffect<Announcements> replayAll(long sinceSeq) {
    return queryResult();
  }

  @Query(value = "SELECT * FROM updates WHERE seq > :sinceSeq", streamUpdates = true)
  public QueryStreamEffect<PresenceUpdate> streamAll(long sinceSeq) {
    return queryStreamResult();
  }

  @Query("SELECT * AS updates FROM updates WHERE userId = :userId AND seq > :sinceSeq ORDER BY seq")
  public QueryEffect<Announcements> replayUser(From from) {
    return queryResult();
  }

  @Query(
      value = "SELECT * FROM updates WHERE userId = :userId AND seq > :sinceSeq",
      streamUpdates = true)
  public QueryStreamEffect<PresenceUpdate> streamUser(From from) {
    return queryStreamResult();
  }
}
