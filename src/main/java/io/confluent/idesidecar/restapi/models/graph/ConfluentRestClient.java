package io.confluent.idesidecar.restapi.models.graph;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.exceptions.CCloudRateLimitException;
import io.confluent.idesidecar.restapi.exceptions.ConnectionNotFoundException;
import io.confluent.idesidecar.restapi.exceptions.ErrorResponse;
import io.confluent.idesidecar.restapi.exceptions.Failure;
import io.confluent.idesidecar.restapi.exceptions.ResourceFetchingException;
import io.confluent.idesidecar.restapi.util.ObjectMapperFactory;
import io.confluent.idesidecar.restapi.util.UriUtil;
import io.confluent.idesidecar.restapi.util.WebClientFactory;
import io.quarkus.logging.Log;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.MultiMap;
import io.vertx.ext.web.client.HttpResponse;
import jakarta.inject.Inject;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * A class that provides a common interface for interacting with APIs that follow the Confluent API
 * conventions.
 */
@RegisterForReflection
public abstract class ConfluentRestClient {

  protected static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getObjectMapper();

  @Inject
  ConnectionStateManager connections;

  @Inject
  WebClientFactory webClientFactory;

  @Inject
  UriUtil uriUtil;

  @ConfigProperty(
      name = "ide-sidecar.connections.ccloud.rate-limit.retry.initial-backoff-ms",
      defaultValue = "500"
  )
  long retryInitialBackoffMs;

  @ConfigProperty(
      name = "ide-sidecar.connections.ccloud.rate-limit.retry.max-backoff-ms",
      defaultValue = "10000"
  )
  long retryMaxBackoffMs;

  @ConfigProperty(
      name = "ide-sidecar.connections.ccloud.rate-limit.retry.max-attempts",
      defaultValue = "5"
  )
  int retryMaxAttempts;

  @ConfigProperty(
      name = "ide-sidecar.connections.ccloud.rate-limit.retry.jitter-factor",
      defaultValue = "0.2"
  )
  double retryJitterFactor;

  /**
   * Acquire a rate-limit permit before making an outbound API call. The default implementation is
   * a no-op; subclasses (e.g., {@link ConfluentCloudRestClient}) override this to gate CCloud
   * requests through a rate limiter.
   */
  protected Uni<Void> acquireRateLimitPermit() {
    return Uni.createFrom().voidItem();
  }

  /**
   * The state used for requesting paginated responses. The first state should just be the original
   * request URL, with the URL of the next page determining whether there are additional pages.
   *
   * <p>Note that Confluent APIs may return empty pages even if there are additional pages
   * remaining. This is because RBAC permissions might filter out all the resources on a page, even
   * though there still may be additional pages of resources. Only the {@code metadata.next} field
   * dictates whether there is another page.
   */
  @RegisterForReflection
  protected static class PaginationState {

    /**
     * The URL of the next page of results.
     */
    volatile String nextUrl;

    public final PageLimits limits;

    private final AtomicInteger nextPageNumber = new AtomicInteger();

    private final AtomicInteger totalItems = new AtomicInteger();

    /**
     * Create a new pagination state, with the initial URL to be called.
     *
     * @param firstUrl the URL to obtain the first page of results; may not be null
     * @param limits   the maximum number of pages to return
     */
    public PaginationState(String firstUrl, PageLimits limits) {
      this.nextUrl = firstUrl;
      this.limits = limits != null ? limits : PageLimits.DEFAULT;
    }

    /**
     * Create a new page of results with the given items. The resulting page may have fewer items
     * than supplied, if the item limit has been exceeded.
     *
     * @param items       the items on the page
     * @param nextPageUrl the URL of the next page, or null if there is no next page
     * @param <T>         the type of items
     * @return the new page
     */
    public <T> PageOfResults<T> newPage(List<T> items, String nextPageUrl) {
      nextUrl = nextPageUrl;
      var itemsRemaining = Math.max(0, limits.maxItems() - totalItems.get());
      var itemsThisPage = Math.min(itemsRemaining, items.size());
      if (itemsThisPage < items.size()) {
        items = items.subList(0, itemsThisPage);
      }
      totalItems.addAndGet(items.size());
      var pageNumber = nextPageNumber.incrementAndGet();
      var pagesRemaining = Math.max(0, limits.maxPages() - pageNumber);
      return new PageOfResults<>(
          items,
          nextPageUrl != null,
          pageNumber,
          pagesRemaining,
          itemsRemaining
      );
    }

    /**
     * Return the number of pages that have been {@link #newPage created} so far.
     *
     * @return the page count
     */
    public int pageCount() {
      return nextPageNumber.get();
    }

    /**
     * Return the total number of items that have been returned on {@link #newPage pages} so far.
     *
     * @return the number of items returned so far
     */
    public int itemsReturned() {
      return totalItems.get();
    }
  }

  /**
   * The limits for {@link #listItems listing items}.
   *
   * @param maxPages the maximum number of pages
   * @param maxItems the maximum number of items
   */
  @RegisterForReflection
  public record PageLimits(
      int maxPages,

      int maxItems
  ) {

    public static final PageLimits DEFAULT = new PageLimits();

    public PageLimits() {
      this(Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    public PageLimits withMaxItems(int maxItems) {
      return new PageLimits(maxPages, maxItems);
    }

    public PageLimits withMaxPages(int maxPages) {
      return new PageLimits(maxPages, maxItems);
    }

    <T> List<T> limit(List<T> items) {
      if (items.size() > maxItems) {
        return items.subList(0, maxItems);
      }
      return items;
    }
  }

  /**
   * An immutable page of results.
   *
   * @param items      the items on the page
   * @param nextPage   whether there is another page
   * @param pageNumber the 1-based page number
   * @param <T>        the type of items on the page
   */
  @RegisterForReflection
  protected record PageOfResults<T>(
      List<T> items,
      boolean nextPage,
      int pageNumber,
      int pagesRemaining,
      int itemsRemaining
  ) {

    public boolean hasNextPage() {
      return nextPage && pagesRemaining > 0 && itemsRemaining > 0;
    }
  }

  /**
   * A parser for a list of (paginated) Confluent API resources.
   *
   * @param <T> the type of items on the page
   */
  protected interface ListParser<T> {

    /**
     * Parse the given JSON response payload into a page of Confluent API resources.
     *
     * @param json  the JSON page; may not be null
     * @param state the current stated used to track pagination; may not be null
     * @return the page of results
     */
    PageOfResults<T> parse(String json, PaginationState state);
  }

  /**
   * A parser for a list of (paginated) Confluent API resources.
   *
   * @param <T> the type of items on the page
   */
  protected interface ItemParser<T> {

    /**
     * Parse the given JSON response payload into a page of Confluent API resources.
     *
     * @param url  the URL that was used to get the JSON response; may not be null
     * @param json the JSON page; may not be null
     * @return the parsed item
     */
    T parse(String url, String json);
  }

  protected record LastRequest(String url, String json) {

  }

  /**
   * Get the Confluent API resources starting with the given URL. If the response is paginated, this
   * stream will continue to get additional pages as needed until there are no more results or the
   * supplied limits are reached.
   *
   * @param headers        the headers to use for the request
   * @param firstUrl       the URL of the first page of Confluent API resources
   * @param limits         the page limits; may be null if there are no limits
   * @param responseParser the parser for the items on the page
   * @param <T>            the type of items being returned
   * @return the stream of items
   */
  protected <T> Multi<T> listItems(
      MultiMap headers,
      String firstUrl,
      PageLimits limits,
      ListParser<T> responseParser
  ) {
    try {
      return Multi
          .createBy()
          .repeating()
          .completionStage(
              () -> new PaginationState(firstUrl, limits),
              // acquire a rate-limit permit, then make request for more results
              state -> acquireRateLimitPermit()
                  .chain(() -> Uni.createFrom().completionStage(
                      webClientFactory
                          .getWebClient()
                          .getAbs(state.nextUrl)
                          .putHeaders(headers)
                          .send()
                          .map(result -> {
                            checkResponse(result, state.nextUrl);
                            return responseParser.parse(result.bodyAsString(), state);
                          })
                          .toCompletionStage()
                  ))
                  .onFailure(CCloudRateLimitException.class)
                  .retry()
                  .withBackOff(
                      Duration.ofMillis(retryInitialBackoffMs),
                      Duration.ofMillis(retryMaxBackoffMs)
                  )
                  .withJitter(retryJitterFactor)
                  .atMost(retryMaxAttempts)
                  .subscribeAsCompletionStage()
          )
          .whilst(PageOfResults::hasNextPage) // include the last page
          .map(PageOfResults::items) // extract the items from the page
          .onItem()
          .disjoint();
    } catch (ConnectionNotFoundException | ResourceFetchingException e) {
      Log.error("Listing items failed with error", e);
      return Multi.createFrom().failure(e);
    }
  }

  /**
   * Get the Confluent API resource at the given URL.
   *
   * @param headers        the headers to use for the request
   * @param responseParser the parser for the items on the page
   * @param <T>            the type of items being returned
   * @return the item
   */
  protected <T> Uni<T> getItem(
      MultiMap headers,
      String url,
      ItemParser<T> responseParser
  ) {
    try {
      return acquireRateLimitPermit()
          .chain(() -> Uni.createFrom().completionStage(
              webClientFactory
                  .getWebClient()
                  .getAbs(url)
                  .putHeaders(headers)
                  .send()
                  .map(result -> {
                    checkResponse(result, url);
                    return responseParser.parse(url, result.bodyAsString());
                  })
                  .toCompletionStage()
          ))
          .onFailure(CCloudRateLimitException.class)
          .retry()
          .withBackOff(
              Duration.ofMillis(retryInitialBackoffMs),
              Duration.ofMillis(retryMaxBackoffMs)
          )
          .withJitter(retryJitterFactor)
          .atMost(retryMaxAttempts);
    } catch (ConnectionNotFoundException | ResourceFetchingException e) {
      Log.error("Getting item failed with error", e);
      return Uni.createFrom().failure(e);
    }
  }

  /**
   * Check the HTTP response status code before attempting to parse the body. Throws
   * {@link CCloudRateLimitException} on 429 (triggering retry) and
   * {@link ResourceFetchingException} on other error status codes.
   *
   * @param response the HTTP response to check
   * @param url      the request URL, for inclusion in error messages
   * @throws CCloudRateLimitException   if the response is 429 Too Many Requests
   * @throws ResourceFetchingException if the response is any other 4xx or 5xx error
   */
  protected void checkResponse(HttpResponse<?> response, String url) {
    int status = response.statusCode();
    if (status == 429) {
      int retryAfter = parseRetryAfterHeader(response);
      Log.warnf("Rate limited by %s (retry-after: %ds)", url, retryAfter);
      throw new CCloudRateLimitException(url, retryAfter);
    }
    if (status >= 400) {
      var body = response.bodyAsString();
      throw parseErrorOrFail(
          url,
          body,
          new RuntimeException("HTTP %d from %s".formatted(status, url))
      );
    }
  }

  /**
   * Parse the Retry-After header from an HTTP response, returning -1 if absent or unparseable.
   */
  private static int parseRetryAfterHeader(HttpResponse<?> response) {
    var header = response.getHeader("Retry-After");
    if (header != null) {
      try {
        return Integer.parseInt(header.trim());
      } catch (NumberFormatException e) {
        // Retry-After can also be an HTTP-date, which we don't parse
      }
    }
    return -1;
  }

  /**
   * Find the connection with the given ID and get the authorization headers.
   *
   * @param connectionId the connection ID
   * @return the headers; never null but possibly empty
   * @throws ConnectionNotFoundException if the connection with the given ID could not be found
   */
  protected MultiMap headersFor(String connectionId) throws ConnectionNotFoundException {
    return MultiMap.caseInsensitiveMultiMap();
  }

  /**
   * Utility method useful for creating type-specific {@link ListParser} methods.
   *
   * @param json     the JSON response payload containing a page of resources
   * @param state    the pagination state of the requests, used to create new pages
   * @param listType the record type of the list response
   * @param <T>      the type of list response object
   * @param <R>      the type of the final representations returned on the page
   * @param <I>      the type of items in the list response object
   * @return the page of results
   */
  protected <T extends ListResponse<I, R>, R, I extends ListItem<R>> PageOfResults<R> parseList(
      String json,
      PaginationState state,
      Class<T> listType
  ) {
    try {
      var response = OBJECT_MAPPER.readValue(json, listType);
      List<R> items = response.data() == null ? List.of() : response
          .data()
          .stream()
          .map(ListItem::toRepresentation)
          .filter(Objects::nonNull)
          .toList();
      return state.newPage(items, response.nextPage());
    } catch (MismatchedInputException e) {
      // A required field was not found, so try parsing as an error
      throw parseErrorOrFail(state.nextUrl, json, e);
    } catch (IOException e) {
      Log.errorf("Could not parse response payload: %s", json);
      throw new ResourceFetchingException(
          "Could not parse response from %s".formatted(state.nextUrl),
          e
      );
    }
  }

  protected <T> T parseRawItem(String url, String json, Class<T> itemType) {
    try {
      return OBJECT_MAPPER.readValue(json, itemType);
    } catch (MismatchedInputException e) {
      // A required field was not found, so try parsing as an error
      throw parseErrorOrFail(url, json, e);
    } catch (IOException e) {
      Log.errorf("Could not parse response payload: %s", json);
      throw new ResourceFetchingException(
          "Could not parse response from %s".formatted(url),
          e
      );
    }
  }

  /**
   * Try to parse the JSON payload as an {@link ErrorResponse} and construct an exception that the
   * caller should throw.
   *
   * @param url           the URL from which the JSON contents were obtained, for inclusion in the
   *                      error message
   * @param json          the JSON contents to parse
   * @param originalError the original error that should be returned if the supplied JSON cannot be
   *                      parsed as an {@link ErrorResponse}
   * @return the exception to throw
   */
  protected ResourceFetchingException parseErrorOrFail(
      String url,
      String json,
      Throwable originalError
  ) {
    Log.infof("Attempting to parse error from response payload: %s", json);
    try {
      var response = OBJECT_MAPPER.readTree(json);
      if (response instanceof ObjectNode obj) {
        if (obj.has("errors") && obj.get("errors") instanceof ArrayNode) {
          var failure = OBJECT_MAPPER.convertValue(response, Failure.class);
          var action = "GET %s".formatted(url);
          return new ResourceFetchingException(failure, action);
        } else if (obj.has("error_code") && obj.has("message")) {
          var error = OBJECT_MAPPER.convertValue(response, ErrorResponse.class);
          var action = "GET %s".formatted(url);
          return new ResourceFetchingException(error, action);
        }
      }
      // continue
    } catch (IOException e) {
      Log.errorf("Ran into error %s when parsing response payload: %s", e.getMessage(), json);
      // continue
    }
    Log.errorf("Could not parse response payload: %s", json);
    return new ResourceFetchingException("Could not parse response", originalError);
  }

  public interface ListResponse<T extends ListItem<R>, R> {

    List<T> data();

    ListMetadata metadata();

    default String nextPage() {
      String nextPage = null;
      if (metadata() != null) {
        nextPage = metadata().nextPage;
        if (nextPage != null && nextPage.isBlank()) {
          nextPage = null;
        }
      }
      return nextPage;
    }

    default boolean hasNextPage() {
      return nextPage() != null;
    }
  }

  public interface ListItem<T> {

    T toRepresentation();
  }

  @RegisterForReflection
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record ListMetadata(
      @JsonProperty(value = "self") String self,
      @JsonProperty(value = "first") String firstPage,
      @JsonProperty(value = "last") String lastPage,
      @JsonProperty(value = "prev") String previousPage,
      @JsonProperty(value = "next") String nextPage,
      @JsonProperty(value = "total_size") int totalSize
  ) {

  }
}
