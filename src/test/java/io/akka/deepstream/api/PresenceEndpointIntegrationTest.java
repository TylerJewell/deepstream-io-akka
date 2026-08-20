package io.akka.deepstream.api;

import static org.assertj.core.api.Assertions.assertThat;

import akka.javasdk.JsonSupport;
import akka.javasdk.testkit.TestKitSupport;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

/**
 * SPEC-001 §3 rules 20 to 25, against a running service.
 *
 * <p>This suite starts a runtime: who-is-online is a view, and a view does not exist outside one.
 */
public class PresenceEndpointIntegrationTest extends TestKitSupport {

  private static final Duration WAIT = Duration.ofSeconds(20);

  private boolean connect(String userId) {
    return httpClient
        .POST("/presence/" + userId + "/connect")
        .responseBodyAs(PresenceEndpoint.Announced.class)
        .invoke()
        .body()
        .announced();
  }

  private boolean disconnect(String userId) {
    return httpClient
        .POST("/presence/" + userId + "/disconnect")
        .responseBodyAs(PresenceEndpoint.Announced.class)
        .invoke()
        .body()
        .announced();
  }

  @SuppressWarnings("unchecked")
  private Map<String, Boolean> query(List<String> userIds) {
    return httpClient
        .POST("/presence/query")
        .withRequestBody(new PresenceEndpoint.NamesQuery(userIds))
        .responseBodyAs(Map.class)
        .invoke()
        .body();
  }

  @Test
  public void refusesTheOpenUserId() {
    // Rule 20: the id deepstream gives a client that never named itself is not a presence.
    var response = httpClient.POST("/presence/OPEN/connect").invoke();
    assertThat(response.httpResponse().status().intValue()).isEqualTo(400);

    assertThat(query(List.of("OPEN"))).containsEntry("OPEN", false);
  }

  @Test
  public void answersBothQueryFormsIncludingTheAsker() {
    String alice = "alice-" + UUID.randomUUID();
    String bob = "bob-" + UUID.randomUUID();

    connect(alice);
    connect(bob);

    // Rule 25: a yes or no per name, the asker included, and a name never seen answers false.
    Awaitility.await()
        .atMost(WAIT)
        .untilAsserted(
            () ->
                assertThat(query(List.of(alice, "carol-never-seen", bob)))
                    .containsExactly(
                        Map.entry(alice, true),
                        Map.entry("carol-never-seen", false),
                        Map.entry(bob, true)));

    // Rule 24: the same answer from the other query — the asker is not spliced out (OD-5).
    @SuppressWarnings("unchecked")
    List<String> online =
        httpClient.GET("/presence").responseBodyAs(List.class).invoke().body();
    assertThat(online).contains(alice, bob);

    disconnect(alice);
    Awaitility.await()
        .atMost(WAIT)
        .untilAsserted(() -> assertThat(query(List.of(alice))).containsEntry(alice, false));
  }

  @Test
  public void announcesOnceForAUserWithTwoConnections() {
    String alice = "alice-" + UUID.randomUUID();

    // Rules 21, 22: only the transitions announce.
    assertThat(connect(alice)).isTrue();
    assertThat(connect(alice)).isFalse();
    assertThat(disconnect(alice)).isFalse();
    assertThat(disconnect(alice)).isTrue();

    // Rule 23: a disconnect with nothing open changes nothing.
    assertThat(disconnect(alice)).isFalse();

    var events =
        testKit.getSelfSseRouteTester().receiveFirstN("/presence/" + alice + "/subscribe", 2, WAIT);
    assertThat(events).hasSize(2);
    assertThat(events.stream().map(e -> decode(e.getData()).online())).containsExactly(true, false);
    assertThat(events.stream().map(e -> e.getId().get())).containsExactly("1", "2");
  }

  @Test
  public void resumesThePresenceStreamFromWhereASubscriberGotTo() {
    String alice = "alice-" + UUID.randomUUID();
    connect(alice);
    disconnect(alice);
    connect(alice);

    var first =
        testKit.getSelfSseRouteTester().receiveFirstN("/presence/" + alice + "/subscribe", 1, WAIT);
    String lastSeen = first.get(0).getId().get();

    // Rule 19 for presence: the same resume the record stream gets.
    var resumed =
        testKit
            .getSelfSseRouteTester()
            .receiveNFromOffset("/presence/" + alice + "/subscribe", 2, lastSeen, WAIT);
    assertThat(resumed.stream().map(e -> e.getId().get())).containsExactly("2", "3");
    assertThat(resumed.stream().map(e -> decode(e.getData()).online())).containsExactly(false, true);
  }

  private static io.akka.deepstream.domain.PresenceUpdate decode(String json) {
    return JsonSupport.decodeJson(
        io.akka.deepstream.domain.PresenceUpdate.class,
        json.getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }
}
