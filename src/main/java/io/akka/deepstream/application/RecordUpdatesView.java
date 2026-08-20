package io.akka.deepstream.application;

import akka.javasdk.annotations.Component;
import akka.javasdk.annotations.Consume;
import akka.javasdk.annotations.Query;
import akka.javasdk.view.TableUpdater;
import akka.javasdk.view.View;
import io.akka.deepstream.domain.RecordUpdate;
import java.util.List;

/**
 * The subscriber stream for one record — SPEC-001 §3 rules 16 to 19.
 *
 * <p>Two queries, because a resumable stream is two different jobs. {@link #replay} hands back,
 * in order, the announcements a subscriber missed while it was away; {@link #stream} pushes what
 * happens from there on, so a subscriber is never asking whether anything changed (R1). The
 * endpoint runs the first and then the second, which is what lets a reconnecting subscriber be
 * given the gap before anything new. The source has no equivalent: a reconnecting client there is
 * sent nothing and has to re-read (OD-1).
 *
 * <p>A streaming query may not carry an ORDER BY — the runtime refuses it — which is why the
 * ordered half is the plain query and not this one.
 */
@Component(id = "record-updates")
public class RecordUpdatesView extends View {

  @Consume.FromKeyValueEntity(RecordUpdateEntity.class)
  public static class Updates extends TableUpdater<RecordUpdate> {
    public Effect<RecordUpdate> onUpdate(RecordUpdate update) {
      return effects().updateRow(update);
    }
  }

  /** A view query takes at most one argument, so the two bounds travel together. */
  public record From(String name, long sinceSeq) {}

  public record Announcements(List<RecordUpdate> updates) {}

  @Query("SELECT * AS updates FROM updates WHERE name = :name AND seq > :sinceSeq ORDER BY seq")
  public QueryEffect<Announcements> replay(From from) {
    return queryResult();
  }

  @Query(value = "SELECT * FROM updates WHERE name = :name AND seq > :sinceSeq", streamUpdates = true)
  public QueryStreamEffect<RecordUpdate> stream(From from) {
    return queryStreamResult();
  }
}
