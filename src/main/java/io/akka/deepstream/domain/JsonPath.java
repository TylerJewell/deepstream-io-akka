package io.akka.deepstream.domain;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Reading and writing a value inside a record by a dotted path — SPEC-001 §3 rules 10, 11, 12, 13,
 * 14, 15 and 28.
 *
 * <p>Every write returns a new map rather than editing the one it was given. The record a command
 * handler is deciding about must survive a rejected write unchanged (rule 7), and a path write
 * that fails part-way through cannot be undone in place.
 *
 * <p>{@code __proto__}, {@code constructor} and {@code prototype} are refused in any segment, and
 * a bracketed token that is not a non-negative integer is refused outright rather than coerced.
 */
public final class JsonPath {

  private static final Set<String> FORBIDDEN = Set.of("__proto__", "constructor", "prototype");

  private JsonPath() {}

  /** A path token: either a key in an object or an index into an array. */
  private sealed interface Token {
    record Key(String name) implements Token {}

    record Index(int at) implements Token {}
  }

  public static boolean isValid(String path) {
    try {
      return !tokenize(path).isEmpty();
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  /** Rule 15: a whole-record write may not smuggle a forbidden key in at the top level. */
  public static boolean hasOnlySafeKeys(Map<String, Object> data) {
    if (data == null) {
      return true;
    }
    for (String key : data.keySet()) {
      if (FORBIDDEN.contains(key)) {
        return false;
      }
    }
    return true;
  }

  /** Rule 10. Throws {@link IllegalArgumentException} if the path is not one rule 13/14 allows. */
  public static Map<String, Object> set(Map<String, Object> record, String path, Object value) {
    Map<String, Object> root = copyMap(record);
    writeInto(root, path, value, false);
    return root;
  }

  /** Rule 11. Throws {@link IllegalArgumentException} if the path is not one rule 13/14 allows. */
  public static Map<String, Object> erase(Map<String, Object> record, String path) {
    Map<String, Object> root = copyMap(record);
    writeInto(root, path, null, true);
    return root;
  }

  /**
   * Rule 12. One copy of the record for the whole batch rather than one per operation: the copy
   * exists so a batch that throws part-way leaves the caller's record untouched, and one copy is
   * enough for that whatever the batch's length.
   */
  public static Map<String, Object> setAll(Map<String, Object> record, List<PatchOp> ops) {
    Map<String, Object> root = copyMap(record);
    for (PatchOp op : ops) {
      writeInto(root, op.path(), op.data(), false);
    }
    return root;
  }

  /**
   * Rule 28. How large the record would be written out as JSON, near enough to enforce a ceiling
   * with. Counted rather than serialised: the answer is only ever compared against a limit, and
   * building the string to measure it would double the cost of every write.
   */
  public static long approximateJsonSize(Object value) {
    return switch (value) {
      case null -> 4;
      case Map<?, ?> map -> {
        long size = 2;
        for (var entry : map.entrySet()) {
          size += String.valueOf(entry.getKey()).length() + 4 + approximateJsonSize(entry.getValue());
        }
        yield size;
      }
      case List<?> list -> {
        long size = 2;
        for (Object element : list) {
          size += approximateJsonSize(element) + 1;
        }
        yield size;
      }
      case String text -> text.length() + 2;
      default -> String.valueOf(value).length();
    };
  }

  /** Writes into a root the caller already owns; nothing here is shared with anyone else's map. */
  private static void writeInto(Object root, String path, Object value, boolean remove) {
    List<Token> tokens = tokenize(path);
    if (tokens.isEmpty()) {
      throw new IllegalArgumentException("path names nothing: '" + path + "'");
    }
    Object node = root;
    for (int i = 0; i < tokens.size() - 1; i++) {
      node = descend(node, tokens.get(i), tokens.get(i + 1));
    }
    place(node, tokens.get(tokens.size() - 1), value, remove);
  }

  /**
   * Returns the child at {@code token}, replacing it with a fresh container when what is there
   * cannot be written into. The shape of the container follows the token after it: an index wants
   * a list, a key wants a map.
   */
  private static Object descend(Object node, Token token, Token next) {
    Object existing = read(node, token);
    boolean fits =
        (next instanceof Token.Index && existing instanceof List<?>)
            || (next instanceof Token.Key && existing instanceof Map<?, ?>);
    if (fits) {
      return existing;
    }
    Object child =
        next instanceof Token.Index ? new ArrayList<>() : new LinkedHashMap<String, Object>();
    place(node, token, child, false);
    return child;
  }

  @SuppressWarnings("unchecked")
  private static Object read(Object node, Token token) {
    if (node instanceof Map<?, ?> m && token instanceof Token.Key key) {
      return ((Map<String, Object>) m).get(key.name());
    }
    if (node instanceof List<?> list && token instanceof Token.Index index) {
      return index.at() < list.size() ? list.get(index.at()) : null;
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  private static void place(Object node, Token token, Object value, boolean remove) {
    if (node instanceof Map<?, ?> m && token instanceof Token.Key key) {
      var target = (Map<String, Object>) m;
      if (remove) {
        target.remove(key.name());
      } else {
        target.put(key.name(), value);
      }
      return;
    }
    if (node instanceof List<?> l && token instanceof Token.Index index) {
      var target = (List<Object>) l;
      if (remove) {
        if (index.at() < target.size()) {
          target.remove(index.at());
        }
        return;
      }
      while (target.size() <= index.at()) {
        target.add(null);
      }
      target.set(index.at(), value);
      return;
    }
    throw new IllegalArgumentException("path does not fit the record's shape");
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> copyMap(Map<String, Object> source) {
    var copy = new LinkedHashMap<String, Object>();
    if (source != null) {
      for (var entry : source.entrySet()) {
        copy.put(entry.getKey(), copyValue(entry.getValue()));
      }
    }
    return copy;
  }

  @SuppressWarnings("unchecked")
  private static Object copyValue(Object value) {
    if (value instanceof Map<?, ?> m) {
      return copyMap((Map<String, Object>) m);
    }
    if (value instanceof List<?> list) {
      var copy = new ArrayList<Object>(list.size());
      for (Object element : list) {
        copy.add(copyValue(element));
      }
      return copy;
    }
    return value;
  }

  private static List<Token> tokenize(String path) {
    if (path == null) {
      throw new IllegalArgumentException("path is missing");
    }
    var tokens = new ArrayList<Token>();
    for (String segment : path.split("\\.", -1)) {
      String part = segment.trim();
      if (part.isEmpty()) {
        continue;
      }
      int bracket = part.indexOf('[');
      String key = bracket == -1 ? part : part.substring(0, bracket);
      if (FORBIDDEN.contains(key)) {
        throw new IllegalArgumentException("forbidden key '" + key + "'");
      }
      if (!key.isEmpty()) {
        tokens.add(new Token.Key(key));
      }
      if (bracket != -1) {
        tokens.addAll(indices(part.substring(bracket)));
      }
    }
    return tokens;
  }

  private static List<Token> indices(String brackets) {
    var tokens = new ArrayList<Token>();
    int at = 0;
    while (at < brackets.length()) {
      if (brackets.charAt(at) != '[') {
        throw new IllegalArgumentException("expected '[' in '" + brackets + "'");
      }
      int close = brackets.indexOf(']', at);
      if (close == -1) {
        throw new IllegalArgumentException("unclosed '[' in '" + brackets + "'");
      }
      String inner = brackets.substring(at + 1, close);
      if (inner.isEmpty() || !inner.chars().allMatch(Character::isDigit)) {
        throw new IllegalArgumentException("array index is not a whole number: '" + inner + "'");
      }
      tokens.add(new Token.Index(Integer.parseInt(inner)));
      at = close + 1;
    }
    return tokens;
  }
}
