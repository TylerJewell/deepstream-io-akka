package io.akka.deepstream.domain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** SPEC-001 §3 rules 10, 11, 13, 14, 15. */
class JsonPathTest {

  private static Map<String, Object> map(Object... kv) {
    var m = new LinkedHashMap<String, Object>();
    for (int i = 0; i < kv.length; i += 2) {
      m.put((String) kv[i], kv[i + 1]);
    }
    return m;
  }

  @Test
  void createsMissingContainersAndDeletesOnErase() {
    assertEquals(
        map("a", map("b", map("c", 1))),
        JsonPath.set(map(), "a.b.c", 1),
        "a missing intermediate becomes an object");

    assertEquals(
        map("a", List.of("x")),
        JsonPath.set(map(), "a[0]", "x"),
        "a numeric next token makes the intermediate an array");

    assertEquals(
        map("a", map("c", 2)),
        JsonPath.erase(map("a", map("b", 1, "c", 2)), "a.b"),
        "erase deletes the key rather than storing a null");

    assertEquals(
        map("a", map("b", 1)),
        JsonPath.set(map("a", 5), "a.b", 1),
        "a scalar on the path is overwritten, not merged");

    assertEquals(
        map("a", map("b", 1)),
        JsonPath.set(map(), "a..b", 1),
        "empty path segments are dropped");
  }

  @Test
  void leavesTheOriginalUntouched() {
    var before = map("a", map("b", 1));
    var after = JsonPath.set(before, "a.b", 2);
    assertEquals(1, ((Map<?, ?>) before.get("a")).get("b"), "the input record is not mutated");
    assertEquals(2, ((Map<?, ?>) after.get("a")).get("b"));
  }

  @Test
  void refusesForbiddenSegmentsAndNonNumericIndices() {
    assertFalse(JsonPath.isValid("__proto__.x"));
    assertFalse(JsonPath.isValid("constructor.x"));
    assertFalse(JsonPath.isValid("prototype"));
    assertFalse(JsonPath.isValid("a.__proto__.b"), "a forbidden key in any segment, not just the first");
    assertFalse(JsonPath.isValid("a.b[__proto__]"), "a non-numeric bracket token is refused (OD-4)");
    assertFalse(JsonPath.isValid("a[-1]"), "a negative index is refused");
    assertFalse(JsonPath.isValid(""), "an empty path names nothing");
    assertTrue(JsonPath.isValid("a.b[2].c"));
    assertTrue(JsonPath.isValid("a[0][1]"));
  }

  @Test
  void refusesForbiddenTopLevelKeysInAWholeRecordWrite() {
    assertFalse(JsonPath.hasOnlySafeKeys(map("__proto__", 1)));
    assertFalse(JsonPath.hasOnlySafeKeys(map("ok", 1, "constructor", 2)));
    assertTrue(JsonPath.hasOnlySafeKeys(map("ok", 1, "also", 2)));
  }

  @Test
  void writesIntoAnExistingArrayWithoutReplacingIt() {
    var list = new ArrayList<Object>(List.of("a", "b"));
    var result = JsonPath.set(map("xs", list), "xs[1]", "B");
    assertEquals(List.of("a", "B"), result.get("xs"));
    assertEquals(List.of("a", "b"), list, "the input list is not mutated");
  }

  @Test
  void growsAnArrayWithNullsWhenTheIndexIsPastTheEnd() {
    var result = JsonPath.set(map("xs", new ArrayList<Object>(List.of("a"))), "xs[2]", "c");
    var xs = (List<?>) result.get("xs");
    assertEquals(3, xs.size());
    assertEquals("a", xs.get(0));
    assertEquals(null, xs.get(1));
    assertEquals("c", xs.get(2));
  }
}
