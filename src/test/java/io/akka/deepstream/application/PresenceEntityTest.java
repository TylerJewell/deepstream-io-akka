package io.akka.deepstream.application;

import static org.assertj.core.api.Assertions.assertThat;

import akka.javasdk.testkit.EventSourcedTestKit;
import io.akka.deepstream.domain.PresenceEvent;
import io.akka.deepstream.domain.PresenceState;
import org.junit.jupiter.api.Test;

/** SPEC-001 §3 rules 20 to 23 — presence is a connection refcount, not a flag. */
class PresenceEntityTest {

  private static EventSourcedTestKit<PresenceState, PresenceEvent, PresenceEntity> kit(String id) {
    return EventSourcedTestKit.of(id, PresenceEntity::new);
  }

  @Test
  void announcesOnFirstConnectionAndLastDisconnectOnly() {
    var kit = kit("alice");

    // Rule 21: the first connection is the join.
    assertThat(kit.method(PresenceEntity::connect).invoke().getReply()).isTrue();
    assertThat(kit.getAllEvents()).hasSize(1);
    assertThat(kit.getAllEvents().get(0)).isInstanceOf(PresenceEvent.Joined.class);

    // Rule 21: the second connection announces nothing, but is counted.
    assertThat(kit.method(PresenceEntity::connect).invoke().getReply()).isFalse();
    assertThat(kit.getState().connections()).isEqualTo(2);

    // Rule 22: closing one of two announces nothing.
    assertThat(kit.method(PresenceEntity::disconnect).invoke().getReply()).isFalse();
    assertThat(kit.getState().connections()).isEqualTo(1);
    assertThat(kit.getState().online()).isTrue();

    // Rule 22: the leave fires when the last one closes.
    assertThat(kit.method(PresenceEntity::disconnect).invoke().getReply()).isTrue();
    assertThat(kit.getState().connections()).isZero();
    assertThat(kit.getState().online()).isFalse();
    assertThat(kit.getAllEvents().get(kit.getAllEvents().size() - 1))
        .isInstanceOf(PresenceEvent.Left.class);
  }

  @Test
  void ignoresADisconnectForAUserWithNoConnections() {
    var kit = kit("ghost");

    // Rule 23.
    assertThat(kit.method(PresenceEntity::disconnect).invoke().getReply()).isFalse();
    assertThat(kit.getAllEvents()).isEmpty();
    assertThat(kit.getState().connections()).isZero();
  }

  @Test
  void reJoinsAfterEveryConnectionHasClosed() {
    var kit = kit("alice");
    kit.method(PresenceEntity::connect).invoke();
    kit.method(PresenceEntity::disconnect).invoke();

    // A user who comes back announces a join again — the refcount, not a one-shot flag.
    assertThat(kit.method(PresenceEntity::connect).invoke().getReply()).isTrue();
    assertThat(kit.getState().online()).isTrue();
  }

  @Test
  void countsEachAnnouncementSoAStreamCanBeResumed() {
    var kit = kit("alice");
    kit.method(PresenceEntity::connect).invoke();
    kit.method(PresenceEntity::connect).invoke();
    kit.method(PresenceEntity::disconnect).invoke();
    kit.method(PresenceEntity::disconnect).invoke();
    kit.method(PresenceEntity::connect).invoke();

    // Rule 18: an announcement carries an identifier, and the identifier only ever grows.
    assertThat(kit.getState().seq()).isEqualTo(3);
  }
}
