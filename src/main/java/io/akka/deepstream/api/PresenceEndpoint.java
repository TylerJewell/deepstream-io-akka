package io.akka.deepstream.api;

import akka.http.javadsl.model.HttpResponse;
import akka.javasdk.annotations.Acl;
import akka.javasdk.annotations.http.Get;
import akka.javasdk.annotations.http.HttpEndpoint;
import akka.javasdk.annotations.http.Post;
import akka.javasdk.client.ComponentClient;
import akka.javasdk.http.AbstractHttpEndpoint;
import akka.javasdk.http.HttpException;
import akka.javasdk.http.HttpResponses;
import akka.stream.javadsl.Source;
import io.akka.deepstream.application.OnlineUsersView;
import io.akka.deepstream.application.PresenceEntity;
import io.akka.deepstream.application.PresenceUpdatesView;
import io.akka.deepstream.domain.PresenceState;
import io.akka.deepstream.domain.PresenceUpdate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Connecting, disconnecting, asking who is online and watching it change — SPEC-001 §3 rules 20 to
 * 25.
 *
 * <p>Both queries answer the same way, the asker included (OD-5): a caller that wants "everyone
 * but me" takes itself out of the list, and does not get a different answer depending on which
 * question it asked.
 */
@HttpEndpoint("/presence")
@Acl(allow = @Acl.Matcher(principal = Acl.Principal.INTERNET))
public class PresenceEndpoint extends AbstractHttpEndpoint {

  private final ComponentClient componentClient;

  public PresenceEndpoint(ComponentClient componentClient) {
    this.componentClient = componentClient;
  }

  public record Announced(boolean announced) {}

  public record NamesQuery(List<String> userIds) {}

  /** Rule 21: true when this connection is the one that announced a join. */
  @Post("/{userId}/connect")
  public Announced connect(String userId) {
    refuseAnonymous(userId);
    return new Announced(
        componentClient.forEventSourcedEntity(userId).method(PresenceEntity::connect).invoke());
  }

  /** Rules 22, 23: true when this disconnect is the one that announced a leave. */
  @Post("/{userId}/disconnect")
  public Announced disconnect(String userId) {
    refuseAnonymous(userId);
    return new Announced(
        componentClient.forEventSourcedEntity(userId).method(PresenceEntity::disconnect).invoke());
  }

  /** Rule 24: everybody online, the asker included. */
  @Get
  public List<String> online() {
    return componentClient.forView().method(OnlineUsersView::online).invoke().users().stream()
        .map(OnlineUsersView.OnlineUserEntry::userId)
        .toList();
  }

  /** Rule 25: a yes or no for each name asked about, in the order asked. */
  @Post("/query")
  public Map<String, Boolean> query(NamesQuery request) {
    List<String> asked = request.userIds() == null ? List.of() : request.userIds();
    Set<String> online =
        asked.isEmpty()
            ? Set.of()
            : componentClient
                .forView()
                .method(OnlineUsersView::onlineAmong)
                .invoke(asked)
                .users()
                .stream()
                .map(OnlineUsersView.OnlineUserEntry::userId)
                .collect(Collectors.toSet());

    var answer = new LinkedHashMap<String, Boolean>();
    for (String userId : asked) {
      answer.put(userId, online.contains(userId));
    }
    return answer;
  }

  /** Rules 18, 21, 22: every join and leave, resumable from where a subscriber got to. */
  @Get("/subscribe")
  public HttpResponse subscribeAll() {
    long since = resumeFrom();
    var missed =
        componentClient.forView().method(PresenceUpdatesView::replayAll).invoke(since).updates();
    var source =
        Source.from(missed)
            .concat(
                componentClient
                    .forView()
                    .stream(PresenceUpdatesView::streamAll)
                    .source(caughtUpAt(missed, since)));
    return HttpResponses.serverSentEvents(source, update -> Long.toString(update.seq()));
  }

  /** The same stream, narrowed to one user. */
  @Get("/{userId}/subscribe")
  public HttpResponse subscribeUser(String userId) {
    long since = resumeFrom();
    var missed =
        componentClient
            .forView()
            .method(PresenceUpdatesView::replayUser)
            .invoke(new PresenceUpdatesView.From(userId, since))
            .updates();
    var source =
        Source.from(missed)
            .concat(
                componentClient
                    .forView()
                    .stream(PresenceUpdatesView::streamUser)
                    .source(new PresenceUpdatesView.From(userId, caughtUpAt(missed, since))));
    return HttpResponses.serverSentEvents(source, update -> Long.toString(update.seq()));
  }

  /** Where the replay got to, and so where the pushed stream picks up without a gap or a repeat. */
  private static long caughtUpAt(List<PresenceUpdate> missed, long since) {
    return missed.isEmpty() ? since : missed.get(missed.size() - 1).seq();
  }

  /** Rule 20: the unauthenticated user id is not a presence, so it may not open one. */
  private static void refuseAnonymous(String userId) {
    if (PresenceState.ANONYMOUS.equals(userId)) {
      throw HttpException.badRequest(
          "'" + PresenceState.ANONYMOUS + "' is the id of a client that never named itself, "
              + "and is not a presence");
    }
  }

  private long resumeFrom() {
    return requestContext()
        .lastSeenSseEventId()
        .or(() -> requestContext().queryParams().getString("since"))
        .map(Long::parseLong)
        .orElse(0L);
  }
}
