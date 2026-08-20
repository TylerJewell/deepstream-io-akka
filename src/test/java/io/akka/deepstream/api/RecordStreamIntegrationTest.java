package io.akka.deepstream.api;

import static org.assertj.core.api.Assertions.assertThat;

import akka.javasdk.JsonSupport;
import akka.javasdk.testkit.TestKitSupport;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * SPEC-001 §3 rules 16 to 19, against a running service.
 *
 * <p>This suite starts a runtime: the subscriber stream is a view fed by a consumer, and neither
 * exists outside one.
 */
public class RecordStreamIntegrationTest extends TestKitSupport {

  private static final Duration WAIT = Duration.ofSeconds(20);

  private String write(String name, RecordEndpoint.WriteBody body) {
    return httpClient
        .POST("/record/" + name)
        .withRequestBody(body)
        .invoke()
        .body()
        .utf8String();
  }

  private static RecordEndpoint.WriteBody update(long version, Map<String, Object> data, boolean upsert) {
    return new RecordEndpoint.WriteBody("update", version, upsert, data, null, null, null);
  }

  private static RecordEndpoint.UpdateMessage decode(String json) {
    return JsonSupport.decodeJson(
        RecordEndpoint.UpdateMessage.class,
        json.getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }

  @Test
  public void streamsAcceptedWritesInVersionOrder() {
    String name = "room-" + UUID.randomUUID();

    write(name, update(-1, Map.of("n", 1), true));
    write(name, update(2, Map.of("n", 2), false));
    // Rule 17: this one loses the race for version 2 and must not appear in the stream.
    write(name, update(2, Map.of("n", "loser"), false));
    write(name, update(3, Map.of("n", 3), false));

    var events =
        testKit.getSelfSseRouteTester().receiveFirstN("/record/" + name + "/subscribe", 3, WAIT);

    assertThat(events).hasSize(3);
    var messages = events.stream().map(e -> decode(e.getData())).toList();
    assertThat(messages.stream().map(RecordEndpoint.UpdateMessage::version))
        .containsExactly(1L, 2L, 3L);
    assertThat(messages.get(1).data()).isEqualTo(Map.of("n", 2));
    assertThat(messages.stream().map(m -> m.data().get("n"))).doesNotContain("loser");

    // Rule 18: every element names where it sits, so a subscriber can say where it got to.
    assertThat(events.stream().map(e -> e.getId().get())).containsExactly("1", "2", "3");
  }

  @Test
  public void resumesFromTheLastSeenVersionAfterADrop() {
    String name = "room-" + UUID.randomUUID();

    write(name, update(-1, Map.of("n", 1), true));
    write(name, update(2, Map.of("n", 2), false));
    write(name, update(3, Map.of("n", 3), false));

    var first =
        testKit.getSelfSseRouteTester().receiveFirstN("/record/" + name + "/subscribe", 1, WAIT);
    String lastSeen = first.get(0).getId().get();
    assertThat(lastSeen).isEqualTo("1");

    // Rule 19: the connection dropped after the first element; reconnecting with that id is sent
    // what came after it, in order. The source sends a reconnecting subscriber nothing (OD-1).
    var resumed =
        testKit
            .getSelfSseRouteTester()
            .receiveNFromOffset("/record/" + name + "/subscribe", 2, lastSeen, WAIT);

    assertThat(resumed.stream().map(e -> e.getId().get())).containsExactly("2", "3");
    assertThat(resumed.stream().map(e -> decode(e.getData()).version())).containsExactly(2L, 3L);
  }

  @Test
  public void answersEveryWriteIncludingTheOnesItRefuses() {
    String name = "room-" + UUID.randomUUID();

    // Rule 6: no record yet, and this write did not ask to create one.
    var missing = httpClient.POST("/record/" + name)
        .withRequestBody(update(1, Map.of("n", 1), false)).invoke();
    assertThat(missing.httpResponse().status().intValue()).isEqualTo(404);

    // OD-2: an accepted write answers too, with the version it became.
    var created = httpClient.POST("/record/" + name)
        .withRequestBody(update(-1, Map.of("n", 1), true)).invoke();
    assertThat(created.httpResponse().status().intValue()).isEqualTo(200);
    assertThat(created.body().utf8String()).contains("\"version\":1");

    // Rules 3, 4: superseded, and told what won.
    var stale = httpClient.POST("/record/" + name)
        .withRequestBody(update(1, Map.of("n", "stale"), false)).invoke();
    assertThat(stale.httpResponse().status().intValue()).isEqualTo(409);
    assertThat(stale.body().utf8String()).contains("version_exists").contains("\"n\":1");

    // Rule 5: a different rejection, and it carries no data.
    var ahead = httpClient.POST("/record/" + name)
        .withRequestBody(update(9, Map.of("n", "ahead"), false)).invoke();
    assertThat(ahead.httpResponse().status().intValue()).isEqualTo(400);
    assertThat(ahead.body().utf8String()).contains("invalid_version");
  }

  @Test
  public void readsAndDeletesThroughTheApi() {
    String name = "room-" + UUID.randomUUID();

    assertThat(httpClient.GET("/record/" + name + "/version").responseBodyAs(Long.class).invoke().body())
        .isEqualTo(-1L);

    write(name, update(-1, Map.of("n", 1), true));
    assertThat(httpClient.GET("/record/" + name + "/version").responseBodyAs(Long.class).invoke().body())
        .isEqualTo(1L);

    httpClient.DELETE("/record/" + name).invoke();

    // Rule 26: a deleted record answers the way a record that never existed does.
    assertThat(httpClient.GET("/record/" + name + "/version").responseBodyAs(Long.class).invoke().body())
        .isEqualTo(-1L);

    // Rule 27, and the delete reaches subscribers as an announcement of its own.
    var events =
        testKit.getSelfSseRouteTester().receiveFirstN("/record/" + name + "/subscribe", 2, WAIT);
    assertThat(events).hasSize(2);
    assertThat(decode(events.get(1).getData()).deleted()).isTrue();
  }

  @Test
  public void appliesAndRefusesPathWrites() {
    String name = "room-" + UUID.randomUUID();
    write(name, update(-1, Map.of("nested", Map.of("n", 1, "keep", 2)), true));

    var patch = new RecordEndpoint.WriteBody("patch", 2, false, null, "nested.n", 9, null);
    assertThat(httpClient.POST("/record/" + name).withRequestBody(patch).invoke()
        .httpResponse().status().intValue()).isEqualTo(200);

    var snapshot = httpClient.GET("/record/" + name)
        .responseBodyAs(RecordEndpoint.RecordBody.class).invoke().body();
    assertThat(snapshot.data()).isEqualTo(Map.of("nested", Map.of("n", 9, "keep", 2)));

    // Rule 14: a bracket token that is not a whole number is refused, not coerced (OD-4).
    var bad = new RecordEndpoint.WriteBody("patch", 3, false, null, "nested[__proto__]", 1, null);
    assertThat(httpClient.POST("/record/" + name).withRequestBody(bad).invoke()
        .httpResponse().status().intValue()).isEqualTo(400);

    // Rule 12: a batch, in order.
    var batch = new RecordEndpoint.WriteBody("patch_multi", 3, false, null, null, null,
        List.of(new RecordEndpoint.PatchOpBody("a", 1), new RecordEndpoint.PatchOpBody("a", 3)));
    assertThat(httpClient.POST("/record/" + name).withRequestBody(batch).invoke()
        .httpResponse().status().intValue()).isEqualTo(200);
    assertThat(httpClient.GET("/record/" + name)
        .responseBodyAs(RecordEndpoint.RecordBody.class)
        .invoke().body().data().get("a")).isEqualTo(3);
  }
}
