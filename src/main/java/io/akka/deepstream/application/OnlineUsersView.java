package io.akka.deepstream.application;

import akka.javasdk.annotations.Component;
import akka.javasdk.annotations.Consume;
import akka.javasdk.annotations.Query;
import akka.javasdk.view.TableUpdater;
import akka.javasdk.view.View;
import io.akka.deepstream.domain.PresenceEvent;
import io.akka.deepstream.domain.PresenceState;
import java.util.List;

/**
 * Who is online — SPEC-001 §3 rules 24, 25.
 *
 * <p>One row per user, holding whether that user has any live connection. The source answers this
 * from a registry reconciled across the cluster; here it is a projection of the same entities that
 * hold the count.
 *
 * <p>The asker is not filtered out of anything (OD-5). The source splices the asker out of one of
 * its two queries and not the other, which makes presence mean two things depending on which one
 * is called.
 */
@Component(id = "online-users")
public class OnlineUsersView extends View {

  public record OnlineUserEntry(String userId, boolean online) {}

  public record OnlineUsers(List<OnlineUserEntry> users) {}

  @Consume.FromEventSourcedEntity(PresenceEntity.class)
  public static class Users extends TableUpdater<OnlineUserEntry> {

    public Effect<OnlineUserEntry> onEvent(PresenceEvent event) {
      String userId = updateContext().eventSubject().orElseThrow();
      return switch (event) {
        case PresenceEvent.Joined ignored -> effects().updateRow(new OnlineUserEntry(userId, true));
        case PresenceEvent.Left ignored -> effects().updateRow(new OnlineUserEntry(userId, false));
        case PresenceEvent.ConnectionOpened ignored -> effects().ignore();
        case PresenceEvent.ConnectionClosed ignored -> effects().ignore();
      };
    }
  }

  /** Rule 24: everybody online, the asker included. */
  @Query("SELECT * AS users FROM users WHERE online = true ORDER BY userId")
  public QueryEffect<OnlineUsers> online() {
    return queryResult();
  }

  /** Rule 25: a yes or no for each name asked about, the asker included. */
  @Query("SELECT * AS users FROM users WHERE userId = ANY(:userIds) AND online = true")
  public QueryEffect<OnlineUsers> onlineAmong(List<String> userIds) {
    return queryResult();
  }
}
