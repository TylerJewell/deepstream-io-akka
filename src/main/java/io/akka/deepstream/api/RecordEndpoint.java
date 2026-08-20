package io.akka.deepstream.api;

import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.StatusCode;
import akka.http.javadsl.model.StatusCodes;
import akka.javasdk.JsonSupport;
import akka.javasdk.annotations.Acl;
import akka.javasdk.annotations.http.Delete;
import akka.javasdk.annotations.http.Get;
import akka.javasdk.annotations.http.HttpEndpoint;
import akka.javasdk.annotations.http.Post;
import akka.javasdk.client.ComponentClient;
import akka.javasdk.http.AbstractHttpEndpoint;
import akka.javasdk.http.HttpResponses;
import akka.stream.javadsl.Source;
import io.akka.deepstream.application.RecordEntity;
import io.akka.deepstream.application.RecordUpdatesView;
import io.akka.deepstream.domain.WriteRequest;
import io.akka.deepstream.domain.WriteResult;
import java.util.Map;

/**
 * Reading, writing, deleting and subscribing to a record — SPEC-001 §3 rules 1 to 19.
 *
 * <p>A record name may contain any character; one containing a slash is percent-encoded in the
 * path, the same as any other path segment.
 *
 * <p>Every write answers (OD-2), and the answer's status says which of the five outcomes it was,
 * so a caller that only reads the status still learns whether to rebase, to fix its request, or to
 * create the record first.
 */
@HttpEndpoint("/record")
@Acl(allow = @Acl.Matcher(principal = Acl.Principal.INTERNET))
public class RecordEndpoint extends AbstractHttpEndpoint {

  private final ComponentClient componentClient;

  public RecordEndpoint(ComponentClient componentClient) {
    this.componentClient = componentClient;
  }

  /** The write as it arrives: one shape for all four actions, so a caller sends one body. */
  public record WriteBody(
      String action,
      long version,
      Boolean upsert,
      Map<String, Object> data,
      String path,
      Object value,
      java.util.List<PatchOpBody> ops) {}

  public record PatchOpBody(String path, Object value) {}

  /** One announcement as a subscriber sees it, with the record's contents back in shape. */
  public record UpdateMessage(
      String name, long seq, long version, Map<String, Object> data, boolean deleted) {}

  @Post("/{name}")
  public HttpResponse write(String name, WriteBody body) {
    WriteRequest request;
    try {
      request = toRequest(body);
    } catch (IllegalArgumentException e) {
      return json(StatusCodes.BAD_REQUEST, new WriteResult.InvalidPath(e.getMessage()));
    }

    var result =
        componentClient
            .forEventSourcedEntity(name)
            .method(RecordEntity::write)
            .invoke(request);

    return switch (result) {
      case WriteResult.Accepted accepted -> HttpResponses.ok(accepted);
      // Rules 3, 4: the caller lost a race and is given what won, so it can rebase.
      case WriteResult.VersionExists exists -> json(StatusCodes.CONFLICT, exists);
      case WriteResult.InvalidVersion invalid -> json(StatusCodes.BAD_REQUEST, invalid);
      case WriteResult.InvalidPath invalid -> json(StatusCodes.BAD_REQUEST, invalid);
      case WriteResult.NotFound notFound -> json(StatusCodes.NOT_FOUND, notFound);
      case WriteResult.TooLarge tooLarge ->
          json(StatusCodes.PAYLOAD_TOO_LARGE, tooLarge);
    };
  }

  private static HttpResponse json(StatusCode status, WriteResult result) {
    return HttpResponse.create()
        .withStatus(status)
        .withEntity(ContentTypes.APPLICATION_JSON, JsonSupport.encodeToAkkaByteString(result));
  }

  /** What a read answers: the record's own shape, not the entity's. */
  public record RecordBody(String name, long version, Map<String, Object> data) {}

  @Get("/{name}")
  public RecordBody read(String name) {
    var snapshot =
        componentClient.forEventSourcedEntity(name).method(RecordEntity::read).invoke();
    return new RecordBody(snapshot.name(), snapshot.version(), snapshot.data());
  }

  /** Rule 26: a record that never existed and one that was deleted both answer -1. */
  @Get("/{name}/version")
  public long version(String name) {
    return componentClient.forEventSourcedEntity(name).method(RecordEntity::head).invoke();
  }

  @Delete("/{name}")
  public long delete(String name) {
    return componentClient.forEventSourcedEntity(name).method(RecordEntity::delete).invoke();
  }

  /**
   * Rules 16 to 19. The stream is pushed, and it starts after the position the caller names —
   * either as {@code ?since=} or, on a reconnect the browser makes itself, as the
   * {@code Last-Event-ID} header. A caller that names nothing gets the record's whole history and
   * then everything new, which is the only starting point that cannot silently skip anything.
   */
  @Get("/{name}/subscribe")
  public HttpResponse subscribe(String name) {
    long since = resumeFrom();

    // The gap first, in order, then everything that follows it. Two queries rather than one:
    // a streaming query may not carry an ORDER BY, so the ordered half has to be the plain one.
    var missed =
        componentClient
            .forView()
            .method(RecordUpdatesView::replay)
            .invoke(new RecordUpdatesView.From(name, since))
            .updates();
    long caughtUpAt = missed.isEmpty() ? since : missed.get(missed.size() - 1).seq();

    var source =
        Source.from(missed)
            .concat(
                componentClient
                    .forView()
                    .stream(RecordUpdatesView::stream)
                    .source(new RecordUpdatesView.From(name, caughtUpAt)))
            .map(RecordEndpoint::toMessage);
    return HttpResponses.serverSentEvents(source, message -> Long.toString(message.seq()));
  }

  private static UpdateMessage toMessage(io.akka.deepstream.domain.RecordUpdate update)
      throws com.fasterxml.jackson.core.JsonProcessingException {
    return new UpdateMessage(
        update.name(),
        update.seq(),
        update.version(),
        JsonSupport.getObjectMapper()
            .readValue(
                update.dataJson(),
                new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {}),
        update.deleted());
  }

  /** Where a subscriber says it got to: the SSE reconnect header first, then an explicit query. */
  private long resumeFrom() {
    return requestContext()
        .lastSeenSseEventId()
        .or(() -> requestContext().queryParams().getString("since"))
        .map(Long::parseLong)
        .orElse(0L);
  }

  private static WriteRequest toRequest(WriteBody body) {
    boolean upsert = Boolean.TRUE.equals(body.upsert());
    String action = body.action() == null ? "update" : body.action();
    return switch (action) {
      case "update" -> new WriteRequest.Update(body.version(), body.data(), upsert);
      case "patch" -> new WriteRequest.Patch(body.version(), body.path(), body.value(), upsert);
      case "erase" -> new WriteRequest.Erase(body.version(), body.path(), upsert);
      case "patch_multi" ->
          new WriteRequest.PatchMulti(
              body.version(),
              body.ops() == null
                  ? java.util.List.of()
                  : body.ops().stream()
                      .map(op -> new io.akka.deepstream.domain.PatchOp(op.path(), op.value()))
                      .toList(),
              upsert);
      default -> throw new IllegalArgumentException("unknown action '" + action + "'");
    };
  }
}
