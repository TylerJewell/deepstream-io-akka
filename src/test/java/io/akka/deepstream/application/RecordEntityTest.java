package io.akka.deepstream.application;

import static org.assertj.core.api.Assertions.assertThat;

import akka.javasdk.testkit.EventSourcedTestKit;
import io.akka.deepstream.domain.PatchOp;
import io.akka.deepstream.domain.RecordEvent;
import io.akka.deepstream.domain.RecordState;
import io.akka.deepstream.domain.WriteRequest;
import io.akka.deepstream.domain.WriteResult;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** SPEC-001 §3 rules 1 to 15 and 26 to 28 — the write decision and what a write does. */
class RecordEntityTest {

  private static EventSourcedTestKit<RecordState, RecordEvent, RecordEntity> kit() {
    return EventSourcedTestKit.of("chat/room-1", RecordEntity::new);
  }

  private static Map<String, Object> map(Object... kv) {
    var m = new LinkedHashMap<String, Object>();
    for (int i = 0; i < kv.length; i += 2) {
      m.put((String) kv[i], kv[i + 1]);
    }
    return m;
  }

  private static WriteResult write(
      EventSourcedTestKit<RecordState, RecordEvent, RecordEntity> kit, WriteRequest request) {
    return kit.method(RecordEntity::write).invoke(request).getReply();
  }

  private static WriteRequest update(long version, Map<String, Object> data, boolean upsert) {
    return new WriteRequest.Update(version, data, upsert);
  }

  @Test
  void acceptsTheNextVersionAndUpsertsOnlyWhenAsked() {
    var kit = kit();

    // Rule 6: a write to a record that does not exist, without upsert, creates nothing.
    assertThat(write(kit, update(1, map("n", 1), false)))
        .isInstanceOf(WriteResult.NotFound.class);
    assertThat(kit.getAllEvents()).isEmpty();

    // Rule 2: version -1 means "whatever comes next" — here, 1.
    assertThat(write(kit, update(-1, map("n", 1), true)))
        .isEqualTo(new WriteResult.Accepted(1));

    // Rule 1: one more than the current version is accepted.
    assertThat(write(kit, update(2, map("n", 2), false)))
        .isEqualTo(new WriteResult.Accepted(2));
    assertThat(kit.getState().version()).isEqualTo(2);
    assertThat(kit.getState().data()).isEqualTo(map("n", 2));

    // Rule 2 again: -1 is never a conflict, however many versions exist.
    assertThat(write(kit, update(-1, map("n", 3), false)))
        .isEqualTo(new WriteResult.Accepted(3));
  }

  @Test
  void rejectsAStaleVersionWithTheWinningVersionAndData() {
    var kit = kit();
    write(kit, update(-1, map("v", "a"), true));
    write(kit, update(2, map("v", "a2"), false));

    // Rules 3, 4, 8: the second writer at version 2 loses, and is told what won.
    var result = write(kit, update(2, map("v", "b2"), false));
    assertThat(result).isEqualTo(new WriteResult.VersionExists(2, map("v", "a2")));

    // A version well behind is the same answer, not a different one.
    assertThat(write(kit, update(1, map("v", "b1"), false)))
        .isEqualTo(new WriteResult.VersionExists(2, map("v", "a2")));
  }

  @Test
  void rejectsAVersionMoreThanOneAheadAsInvalid() {
    var kit = kit();
    write(kit, update(-1, map("v", "a"), true));

    // Rule 5: a different rejection, and it does not carry the data.
    assertThat(write(kit, update(7, map("v", "far"), false)))
        .isEqualTo(new WriteResult.InvalidVersion(1));
  }

  @Test
  void leavesTheRecordUntouchedWhenAWriteIsRejected() {
    var kit = kit();
    write(kit, update(-1, map("v", "a"), true));
    int eventsAfterCreate = kit.getAllEvents().size();

    write(kit, update(1, map("v", "stale"), false));
    write(kit, update(9, map("v", "ahead"), false));
    write(kit, new WriteRequest.Patch(2, "__proto__.x", 1, false));

    // Rule 7: none of the three changed anything, and none of them was persisted.
    assertThat(kit.getState().version()).isEqualTo(1);
    assertThat(kit.getState().data()).isEqualTo(map("v", "a"));
    assertThat(kit.getAllEvents()).hasSize(eventsAfterCreate);
  }

  @Test
  void appliesWholeRecordPathAndEraseWrites() {
    var kit = kit();
    write(kit, update(-1, map("name", "x", "nested", map("n", 1, "keep", 2)), true));

    // Rule 10: a patch writes at its path and leaves the rest alone.
    assertThat(write(kit, new WriteRequest.Patch(2, "nested.n", 9, false)))
        .isEqualTo(new WriteResult.Accepted(2));
    assertThat(kit.getState().data()).isEqualTo(map("name", "x", "nested", map("n", 9, "keep", 2)));

    // Rule 11: an erase deletes the key.
    assertThat(write(kit, new WriteRequest.Erase(3, "nested.n", false)))
        .isEqualTo(new WriteResult.Accepted(3));
    assertThat(kit.getState().data()).isEqualTo(map("name", "x", "nested", map("keep", 2)));

    // Rule 9: an update replaces everything.
    assertThat(write(kit, update(4, map("only", true), false)))
        .isEqualTo(new WriteResult.Accepted(4));
    assertThat(kit.getState().data()).isEqualTo(map("only", true));
  }

  @Test
  void appliesABatchInOrderOrNotAtAll() {
    var kit = kit();
    write(kit, update(-1, map("a", 0), true));

    // Rule 12: in order — the later operation on the same path wins.
    assertThat(
            write(
                kit,
                new WriteRequest.PatchMulti(
                    2,
                    List.of(new PatchOp("a", 1), new PatchOp("b.c", 2), new PatchOp("a", 3)),
                    false)))
        .isEqualTo(new WriteResult.Accepted(2));
    assertThat(kit.getState().data()).isEqualTo(map("a", 3, "b", map("c", 2)));

    // Rule 12: all-or-nothing — a bad operation anywhere leaves the record exactly as it was.
    var before = kit.getState().data();
    assertThat(
            write(
                kit,
                new WriteRequest.PatchMulti(
                    3,
                    List.of(new PatchOp("a", 99), new PatchOp("__proto__.x", 1)),
                    false)))
        .isInstanceOf(WriteResult.InvalidPath.class);
    assertThat(kit.getState().data()).isEqualTo(before);
    assertThat(kit.getState().version()).isEqualTo(2);

    // An empty batch names no change and is refused rather than burning a version.
    assertThat(write(kit, new WriteRequest.PatchMulti(3, List.of(), false)))
        .isInstanceOf(WriteResult.InvalidPath.class);
  }

  @Test
  void refusesAWholeRecordWriteCarryingAForbiddenKey() {
    var kit = kit();
    // Rule 15.
    assertThat(write(kit, update(-1, map("__proto__", map("polluted", true)), true)))
        .isInstanceOf(WriteResult.InvalidPath.class);
    assertThat(kit.getAllEvents()).isEmpty();
  }

  @Test
  void treatsADeletedRecordAsMissing() {
    var kit = kit();
    write(kit, update(-1, map("v", "a"), true));
    kit.method(RecordEntity::delete).invoke();

    // Rule 26: the same answer a record that never existed gives.
    assertThat(kit.method(RecordEntity::head).invoke().getReply()).isEqualTo(-1L);

    // Rule 27: a write to it is NotFound unless it asked to upsert.
    assertThat(write(kit, update(2, map("v", "b"), false)))
        .isInstanceOf(WriteResult.NotFound.class);

    // And an upsert starts the version count again, as a fresh record does.
    assertThat(write(kit, update(-1, map("v", "b"), true)))
        .isEqualTo(new WriteResult.Accepted(1));
  }

  @Test
  void refusesARecordPastTheSizeTheTargetWillReplicate() {
    var kit = kit();
    write(kit, update(-1, map("small", "x"), true));

    // Rule 28: past the ceiling the record would stop replicating, so the write is refused
    // rather than accepted into a state that cannot leave the region it was written in.
    var huge = map("big", "x".repeat((int) io.akka.deepstream.domain.RecordState.MAX_RECORD_BYTES + 1));
    assertThat(write(kit, update(2, huge, false))).isInstanceOf(WriteResult.TooLarge.class);

    // Rule 7 still holds: the refused write left the record exactly as it was.
    assertThat(kit.getState().version()).isEqualTo(1);
    assertThat(kit.getState().data()).isEqualTo(map("small", "x"));
  }

  @Test
  void answersTheCurrentVersionAndDataOnRead() {
    var kit = kit();
    assertThat(kit.method(RecordEntity::head).invoke().getReply()).isEqualTo(-1L);

    write(kit, update(-1, map("v", "a"), true));
    assertThat(kit.method(RecordEntity::head).invoke().getReply()).isEqualTo(1L);

    var snapshot = kit.method(RecordEntity::read).invoke().getReply();
    assertThat(snapshot.version()).isEqualTo(1L);
    assertThat(snapshot.data()).isEqualTo(map("v", "a"));
  }
}
