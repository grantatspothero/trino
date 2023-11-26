/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.dispatcher;

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FluentFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.trino.client.DrainState;
import io.trino.client.QueryError;
import io.trino.client.QueryResults;
import io.trino.client.StatementStats;
import io.trino.execution.ExecutionFailureInfo;
import io.trino.execution.QueryManagerConfig;
import io.trino.execution.QueryState;
import io.trino.server.HttpRequestSessionContextFactory;
import io.trino.server.ServerConfig;
import io.trino.server.SessionContext;
import io.trino.server.StartupStatus;
import io.trino.server.protocol.QueryInfoUrlFactory;
import io.trino.server.protocol.Slug;
import io.trino.server.security.InternalPrincipal;
import io.trino.server.security.ResourceSecurity;
import io.trino.spi.ErrorCode;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.security.Identity;
import io.trino.tracing.TrinoAttributes;
import jakarta.annotation.Nullable;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import jakarta.ws.rs.core.UriBuilder;
import jakarta.ws.rs.core.UriInfo;

import java.net.URI;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Comparators.max;
import static com.google.common.collect.Comparators.min;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.jaxrs.AsyncResponseHandler.bindAsyncResponse;
import static io.trino.client.DrainState.CLUSTER_START_TIME_HEADER;
import static io.trino.client.DrainState.IF_IDLE_FOR_HEADER;
import static io.trino.execution.QueryState.FAILED;
import static io.trino.execution.QueryState.QUEUED;
import static io.trino.server.HttpRequestSessionContextFactory.AUTHENTICATED_IDENTITY;
import static io.trino.server.protocol.QueryInfoUrlFactory.getQueryInfoUri;
import static io.trino.server.protocol.Slug.Context.EXECUTING_QUERY;
import static io.trino.server.protocol.Slug.Context.QUEUED_QUERY;
import static io.trino.server.security.ResourceSecurity.AccessType.AUTHENTICATED_USER;
import static io.trino.server.security.ResourceSecurity.AccessType.PUBLIC;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.SERVER_SHUTTING_DOWN;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static jakarta.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;
import static jakarta.ws.rs.core.Response.Status.BAD_REQUEST;
import static jakarta.ws.rs.core.Response.Status.CONFLICT;
import static jakarta.ws.rs.core.Response.Status.FORBIDDEN;
import static jakarta.ws.rs.core.Response.Status.GONE;
import static jakarta.ws.rs.core.Response.Status.NOT_FOUND;
import static jakarta.ws.rs.core.Response.Status.OK;
import static jakarta.ws.rs.core.Response.Status.PRECONDITION_FAILED;
import static jakarta.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@Path("/v1/statement")
public class QueuedStatementResource
{
    private static final Logger log = Logger.get(QueuedStatementResource.class);
    private static final Duration MAX_WAIT_TIME = new Duration(1, SECONDS);
    private static final Ordering<Comparable<Duration>> WAIT_ORDERING = Ordering.natural().nullsLast();
    private static final Duration NO_DURATION = new Duration(0, MILLISECONDS);

    private final HttpRequestSessionContextFactory sessionContextFactory;
    private final DispatchManager dispatchManager;
    private final Tracer tracer;

    private final QueryInfoUrlFactory queryInfoUrlFactory;

    private final Executor responseExecutor;
    private final ScheduledExecutorService timeoutExecutor;

    private final boolean compressionEnabled;
    private final QueryManager queryManager;
    private final StartupStatus startupStatus;

    @Inject
    public QueuedStatementResource(
            HttpRequestSessionContextFactory sessionContextFactory,
            DispatchManager dispatchManager,
            Tracer tracer,
            DispatchExecutor executor,
            QueryInfoUrlFactory queryInfoUrlTemplate,
            StartupStatus startupStatus,
            ServerConfig serverConfig,
            QueryManagerConfig queryManagerConfig)
    {
        this.sessionContextFactory = requireNonNull(sessionContextFactory, "sessionContextFactory is null");
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.responseExecutor = executor.getExecutor();
        this.timeoutExecutor = executor.getScheduledExecutor();
        this.queryInfoUrlFactory = requireNonNull(queryInfoUrlTemplate, "queryInfoUrlTemplate is null");
        this.startupStatus = requireNonNull(startupStatus, "startupStatus is null");
        this.compressionEnabled = serverConfig.isQueryResultsCompressionEnabled();
        queryManager = new QueryManager(queryManagerConfig.getClientTimeout());
    }

    @PostConstruct
    public void start()
    {
        queryManager.initialize(dispatchManager);
    }

    @PreDestroy
    public void stop()
    {
        queryManager.destroy();
    }

    @ResourceSecurity(AUTHENTICATED_USER)
    @POST
    @Produces(APPLICATION_JSON)
    public Response postStatement(
            String statement,
            @Context HttpServletRequest servletRequest,
            @Context HttpHeaders httpHeaders,
            @Context UriInfo uriInfo)
    {
        if (isNullOrEmpty(statement)) {
            throw badRequest(BAD_REQUEST, "SQL statement is empty");
        }

        Query query = registerQueryIfNeeded(servletRequest, httpHeaders, sessionContext ->
                new Query(statement, sessionContext, dispatchManager, queryInfoUrlFactory, tracer));

        return createQueryResultsResponse(query.getQueryResults(query.getLastToken(), uriInfo));
    }

    @ResourceSecurity(AUTHENTICATED_USER)
    @PUT
    @Path("queued/{queryId}/{slug}")
    @Produces(APPLICATION_JSON)
    public Response putStatement(
            @PathParam("queryId") QueryId queryId,
            @PathParam("slug") String slug,
            String statement,
            @Context HttpServletRequest servletRequest,
            @Context HttpHeaders httpHeaders,
            @Context UriInfo uriInfo)
    {
        if (isNullOrEmpty(statement)) {
            throw badRequest(BAD_REQUEST, "SQL statement is empty");
        }
        if (isNullOrEmpty(slug)) {
            throw badRequest(BAD_REQUEST, "Slug is empty");
        }
        if (!startupStatus.isStartupComplete()) {
            throw badRequest(SERVICE_UNAVAILABLE, "Trino server is still initializing");
        }

        Query query = registerQueryIfNeeded(servletRequest, httpHeaders, sessionContext ->
                new Query(statement, queryId, Optional.of(slug), sessionContext, dispatchManager, queryInfoUrlFactory, tracer));

        if (!query.getSubmitSlug().equals(Optional.of(slug)) || (query.getLastToken() != 0)) {
            throw badRequest(CONFLICT, "Conflict with existing query");
        }

        return createQueryResultsResponse(query.getQueryResults(query.getLastToken(), uriInfo));
    }

    private Query registerQueryIfNeeded(HttpServletRequest servletRequest, HttpHeaders httpHeaders, Function<SessionContext, Query> queryFactory)
    {
        Optional<String> remoteAddress = Optional.ofNullable(servletRequest.getRemoteAddr());
        Optional<Identity> identity = Optional.ofNullable((Identity) servletRequest.getAttribute(AUTHENTICATED_IDENTITY));
        if (identity.flatMap(Identity::getPrincipal).map(InternalPrincipal.class::isInstance).orElse(false)) {
            throw badRequest(FORBIDDEN, "Internal communication can not be used to start a query");
        }

        MultivaluedMap<String, String> headers = httpHeaders.getRequestHeaders();

        SessionContext sessionContext = sessionContextFactory.createSessionContext(headers, remoteAddress, identity);
        Query query = queryManager.registerQuery(() -> queryFactory.apply(sessionContext))
                .orElseThrow(() -> badRequest(GONE, "Server is shutting down"));

        // let authentication filter know that identity lifecycle has been handed off
        servletRequest.setAttribute(AUTHENTICATED_IDENTITY, null);

        return query;
    }

    @ResourceSecurity(PUBLIC)
    @GET
    @Path("drainState")
    @Produces(APPLICATION_JSON)
    public DrainState getDrainState()
    {
        return queryManager.getDrainState();
    }

    @ResourceSecurity(PUBLIC)
    @PUT
    @Path("drainState")
    @Consumes(APPLICATION_JSON)
    public Response putDrainState(DrainState drainState, @Context HttpHeaders httpHeaders)
    {
        DrainState.Type type = drainState.getType();
        Optional<Duration> maxDrainTime = drainState.getMaxDrainTime();

        switch (type) {
            case UNDRAINED:
                return Response.status(queryManager.isDrainStarted() ? CONFLICT : OK).build();
            case DRAINING:
                return Response.status(queryManager.trySetDraining(maxDrainTime) ? OK : CONFLICT).build();
            case DRAINED:
                Optional<Duration> ifIdleFor = extractIfIdleFor(httpHeaders);
                Optional<Instant> clusterStartTime = extractClusterStartTime(httpHeaders);
                return Response.status(queryManager.trySetDrained(ifIdleFor, clusterStartTime) ? OK : PRECONDITION_FAILED).build();
            default:
                throw new VerifyException("Unknown drain state type: " + type);
        }
    }

    private static Optional<Duration> extractIfIdleFor(HttpHeaders httpHeaders)
    {
        String headerString = httpHeaders.getHeaderString(IF_IDLE_FOR_HEADER);
        if (isNullOrEmpty(headerString)) {
            return Optional.empty();
        }
        try {
            return Optional.of(Duration.valueOf(headerString));
        }
        catch (IllegalArgumentException e) {
            throw badRequest(BAD_REQUEST, format("Invalid %s header value", IF_IDLE_FOR_HEADER));
        }
    }

    private static Optional<Instant> extractClusterStartTime(HttpHeaders httpHeaders)
    {
        String headerString = httpHeaders.getHeaderString(CLUSTER_START_TIME_HEADER);
        if (isNullOrEmpty(headerString)) {
            return Optional.empty();
        }
        try {
            return Optional.of(Instant.parse(headerString));
        }
        catch (DateTimeParseException e) {
            throw badRequest(BAD_REQUEST, format("Invalid %s header value", CLUSTER_START_TIME_HEADER));
        }
    }

    @ResourceSecurity(PUBLIC)
    @GET
    @Path("queued/{queryId}/{slug}/{token}")
    @Produces(APPLICATION_JSON)
    public void getStatus(
            @PathParam("queryId") QueryId queryId,
            @PathParam("slug") String slug,
            @PathParam("token") long token,
            @QueryParam("maxWait") Duration maxWait,
            @Context UriInfo uriInfo,
            @Suspended AsyncResponse asyncResponse)
    {
        Query query = getQuery(queryId, slug, token);

        ListenableFuture<Response> future = getStatus(query, token, maxWait, uriInfo);
        bindAsyncResponse(asyncResponse, future, responseExecutor);
    }

    private ListenableFuture<Response> getStatus(Query query, long token, Duration maxWait, UriInfo uriInfo)
    {
        long waitMillis = WAIT_ORDERING.min(MAX_WAIT_TIME, maxWait).toMillis();

        return FluentFuture.from(query.waitForDispatched())
                // wait for query to be dispatched, up to the wait timeout
                .withTimeout(waitMillis, MILLISECONDS, timeoutExecutor)
                .catching(TimeoutException.class, ignored -> null, directExecutor())
                // when state changes, fetch the next result
                .transform(ignored -> query.getQueryResults(token, uriInfo), responseExecutor)
                .transform(this::createQueryResultsResponse, directExecutor());
    }

    @ResourceSecurity(PUBLIC)
    @DELETE
    @Path("queued/{queryId}/{slug}/{token}")
    @Produces(APPLICATION_JSON)
    public Response cancelQuery(
            @PathParam("queryId") QueryId queryId,
            @PathParam("slug") String slug,
            @PathParam("token") long token)
    {
        getQuery(queryId, slug, token)
                .cancel();
        return Response.noContent().build();
    }

    private Query getQuery(QueryId queryId, String slug, long token)
    {
        Query query = queryManager.getQuery(queryId);
        if (query == null || !query.getSlug().isValid(QUEUED_QUERY, slug, token)) {
            throw badRequest(NOT_FOUND, "Query not found");
        }
        return query;
    }

    private Response createQueryResultsResponse(QueryResults results)
    {
        Response.ResponseBuilder builder = Response.ok(results);
        if (!compressionEnabled) {
            builder.encoding("identity");
        }
        return builder.build();
    }

    private static URI getQueuedUri(QueryId queryId, Slug slug, long token, UriInfo uriInfo)
    {
        return uriInfo.getBaseUriBuilder()
                .replacePath("/v1/statement/queued/")
                .path(queryId.toString())
                .path(slug.makeSlug(QUEUED_QUERY, token))
                .path(String.valueOf(token))
                .replaceQuery("")
                .build();
    }

    private static QueryResults createQueryResults(
            QueryId queryId,
            URI nextUri,
            Optional<QueryError> queryError,
            UriInfo uriInfo,
            Optional<URI> queryInfoUrl,
            Duration elapsedTime,
            Duration queuedTime)
    {
        QueryState state = queryError.map(error -> FAILED).orElse(QUEUED);
        return new QueryResults(
                queryId.toString(),
                getQueryInfoUri(queryInfoUrl, queryId, uriInfo),
                null,
                nextUri,
                null,
                null,
                StatementStats.builder()
                        .setState(state.toString())
                        .setQueued(state == QUEUED)
                        .setProgressPercentage(OptionalDouble.empty())
                        .setRunningPercentage(OptionalDouble.empty())
                        .setElapsedTimeMillis(elapsedTime.toMillis())
                        .setQueuedTimeMillis(queuedTime.toMillis())
                        .build(),
                queryError.orElse(null),
                ImmutableList.of(),
                null,
                null);
    }

    private static WebApplicationException badRequest(Status status, String message)
    {
        throw new WebApplicationException(
                Response.status(status)
                        .type(TEXT_PLAIN_TYPE)
                        .entity(message)
                        .build());
    }

    private static final class Query
    {
        private final String query;
        private final SessionContext sessionContext;
        private final DispatchManager dispatchManager;
        private final QueryId queryId;
        private final Optional<URI> queryInfoUrl;
        private final Span querySpan;
        private final Slug slug = Slug.createNew();
        private final Optional<String> submitSlug;
        private final AtomicLong lastToken = new AtomicLong();

        private final long initTime = System.nanoTime();
        private final AtomicReference<Boolean> submissionGate = new AtomicReference<>();
        private final SettableFuture<Void> creationFuture = SettableFuture.create();
        private final ListenableFuture<Void> completionFuture;

        public Query(String query, SessionContext sessionContext, DispatchManager dispatchManager, QueryInfoUrlFactory queryInfoUrlFactory, Tracer tracer)
        {
            this(query, dispatchManager.createQueryId(), Optional.empty(), sessionContext, dispatchManager, queryInfoUrlFactory, tracer);
        }

        public Query(
                String query,
                QueryId queryId,
                Optional<String> submitSlug,
                SessionContext sessionContext,
                DispatchManager dispatchManager,
                QueryInfoUrlFactory queryInfoUrlFactory,
                Tracer tracer)
        {
            this.query = requireNonNull(query, "query is null");
            this.sessionContext = requireNonNull(sessionContext, "sessionContext is null");
            this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
            this.queryId = requireNonNull(queryId, "queryId is null");
            this.submitSlug = requireNonNull(submitSlug, "submitSlug is null");
            requireNonNull(queryInfoUrlFactory, "queryInfoUrlFactory is null");
            this.queryInfoUrl = queryInfoUrlFactory.getQueryInfoUrl(queryId);
            completionFuture = nonCancellationPropagating(transformAsync(
                    creationFuture,
                    ignored -> dispatchManager.tryGetCompletionFuture(queryId).orElse(immediateVoidFuture()),
                    directExecutor()));
            requireNonNull(tracer, "tracer is null");
            this.querySpan = tracer.spanBuilder("query")
                    .setAttribute(TrinoAttributes.QUERY_ID, queryId.toString())
                    .startSpan();
        }

        public QueryId getQueryId()
        {
            return queryId;
        }

        public Slug getSlug()
        {
            return slug;
        }

        public Optional<String> getSubmitSlug()
        {
            return submitSlug;
        }

        public long getLastToken()
        {
            return lastToken.get();
        }

        public boolean tryAbandonSubmissionWithTimeout(Duration querySubmissionTimeout)
        {
            return Duration.nanosSince(initTime).compareTo(querySubmissionTimeout) >= 0 && submissionGate.compareAndSet(null, false);
        }

        public boolean isSubmissionAbandoned()
        {
            return Boolean.FALSE.equals(submissionGate.get());
        }

        public boolean isCreated()
        {
            return creationFuture.isDone();
        }

        private ListenableFuture<Void> waitForDispatched()
        {
            submitIfNeeded();
            if (!creationFuture.isDone()) {
                return nonCancellationPropagating(creationFuture);
            }
            // otherwise, wait for the query to finish
            return dispatchManager.waitForDispatched(queryId);
        }

        private void submitIfNeeded()
        {
            if (submissionGate.compareAndSet(null, true)) {
                querySpan.addEvent("submit");
                creationFuture.setFuture(dispatchManager.createQuery(queryId, querySpan, slug, sessionContext, query));
            }
        }

        public ListenableFuture<Void> getCompletionFuture()
        {
            return completionFuture;
        }

        public QueryResults getQueryResults(long token, UriInfo uriInfo)
        {
            long lastToken = this.lastToken.get();
            // token should be the last token or the next token
            if (token != lastToken && token != lastToken + 1) {
                throw new WebApplicationException(GONE);
            }
            // advance (or stay at) the token
            this.lastToken.compareAndSet(lastToken, token);

            // if query submission has not finished, return simple empty result
            if (!creationFuture.isDone()) {
                return createQueryResults(
                        token + 1,
                        uriInfo,
                        DispatchInfo.queued(NO_DURATION, NO_DURATION));
            }

            DispatchInfo dispatchInfo = dispatchManager.getDispatchInfo(queryId)
                    // query should always be found, but it may have just been determined to be abandoned
                    .orElseThrow(() -> new WebApplicationException(Response
                            .status(NOT_FOUND)
                            .build()));

            return createQueryResults(token + 1, uriInfo, dispatchInfo);
        }

        public void cancel()
        {
            creationFuture.addListener(() -> dispatchManager.cancelQuery(queryId), directExecutor());
        }

        public void failOrAbandon(Throwable cause)
        {
            creationFuture.addListener(() -> dispatchManager.failQuery(queryId, cause), directExecutor());
            // Abandon the submission if it has not occurred yet
            submissionGate.compareAndSet(null, false);
        }

        public void destroy()
        {
            querySpan.setStatus(StatusCode.ERROR).end();
            sessionContext.getIdentity().destroy();
        }

        private QueryResults createQueryResults(long token, UriInfo uriInfo, DispatchInfo dispatchInfo)
        {
            URI nextUri = getNextUri(token, uriInfo, dispatchInfo);

            Optional<QueryError> queryError = dispatchInfo.getFailureInfo()
                    .map(this::toQueryError);

            return QueuedStatementResource.createQueryResults(
                    queryId,
                    nextUri,
                    queryError,
                    uriInfo,
                    queryInfoUrl,
                    dispatchInfo.getElapsedTime(),
                    dispatchInfo.getQueuedTime());
        }

        private URI getNextUri(long token, UriInfo uriInfo, DispatchInfo dispatchInfo)
        {
            // if failed, query is complete
            if (dispatchInfo.getFailureInfo().isPresent()) {
                return null;
            }
            // if dispatched, redirect to new uri
            return dispatchInfo.getCoordinatorLocation()
                    .map(coordinatorLocation -> getRedirectUri(coordinatorLocation, uriInfo))
                    .orElseGet(() -> getQueuedUri(queryId, slug, token, uriInfo));
        }

        private URI getRedirectUri(CoordinatorLocation coordinatorLocation, UriInfo uriInfo)
        {
            URI coordinatorUri = coordinatorLocation.getUri(uriInfo);
            return UriBuilder.fromUri(coordinatorUri)
                    .replacePath("/v1/statement/executing")
                    .path(queryId.toString())
                    .path(slug.makeSlug(EXECUTING_QUERY, 0))
                    .path("0")
                    .build();
        }

        private QueryError toQueryError(ExecutionFailureInfo executionFailureInfo)
        {
            ErrorCode errorCode;
            if (executionFailureInfo.getErrorCode() != null) {
                errorCode = executionFailureInfo.getErrorCode();
            }
            else {
                errorCode = GENERIC_INTERNAL_ERROR.toErrorCode();
                log.warn("Failed query %s has no error code", queryId);
            }

            return new QueryError(
                    firstNonNull(executionFailureInfo.getMessage(), "Internal error"),
                    null,
                    errorCode.getCode(),
                    errorCode.getName(),
                    errorCode.getType().toString(),
                    executionFailureInfo.getErrorLocation(),
                    executionFailureInfo.toFailureInfo());
        }
    }

    @ThreadSafe
    private static class QueryManager
    {
        private static final Duration QUERY_LINGER_TIME = new Duration(30, TimeUnit.SECONDS);

        private final ConcurrentMap<QueryId, Query> queries = new ConcurrentHashMap<>();
        private final Set<QueryId> activeQueries = Sets.newConcurrentHashSet();
        private final AtomicReference<Long> coordinatorIdleStartNanos = new AtomicReference<>(System.nanoTime());
        private final SettableFuture<Void> idleAfterDrainingFuture = SettableFuture.create();
        private final ScheduledExecutorService scheduledExecutorService = newSingleThreadScheduledExecutor(daemonThreadsNamed("drain-state-query-manager"));

        private final Duration querySubmissionTimeout;

        @GuardedBy("this")
        private boolean drainStarted;

        @GuardedBy("this")
        private boolean drainCompleted;

        @GuardedBy("this")
        private Optional<DrainTimeoutTask> drainTimeoutTask = Optional.empty();

        public QueryManager(Duration querySubmissionTimeout)
        {
            this.querySubmissionTimeout = requireNonNull(querySubmissionTimeout, "querySubmissionTimeout is null");
        }

        public void initialize(DispatchManager dispatchManager)
        {
            scheduledExecutorService.scheduleWithFixedDelay(() -> {
                try {
                    syncWith(dispatchManager);
                }
                catch (Throwable e) {
                    // ignore to avoid getting unscheduled
                    log.error(e, "Unexpected error synchronizing with dispatch manager");
                }
            }, 200, 200, MILLISECONDS);
        }

        public void destroy()
        {
            scheduledExecutorService.shutdownNow();
        }

        private void syncWith(DispatchManager dispatchManager)
        {
            queries.forEach((queryId, query) -> {
                if (query.getCompletionFuture().isDone()) {
                    activeQueries.remove(queryId);
                }

                if (shouldBePurged(dispatchManager, query)) {
                    removeQuery(queryId);
                }
            });

            updateIdlenessState();
        }

        private boolean shouldBePurged(DispatchManager dispatchManager, Query query)
        {
            if (query.isSubmissionAbandoned()) {
                // Query submission was explicitly abandoned
                return true;
            }
            if (query.tryAbandonSubmissionWithTimeout(querySubmissionTimeout)) {
                // Query took too long to be submitted by the client
                return true;
            }
            if (query.isCreated() && !dispatchManager.isQueryRegistered(query.getQueryId())) {
                // Query was created in the DispatchManager, and DispatchManager has already purged the query
                return true;
            }
            return false;
        }

        private void removeQuery(QueryId queryId)
        {
            activeQueries.remove(queryId);
            Optional.ofNullable(queries.remove(queryId))
                    .ifPresent(QueryManager::destroyQuietly);
        }

        private static void destroyQuietly(Query query)
        {
            try {
                query.destroy();
            }
            catch (Throwable t) {
                log.error(t, "Error destroying query");
            }
        }

        public synchronized Optional<Query> registerQuery(Supplier<Query> queryFactory)
        {
            if (drainStarted) {
                return Optional.empty();
            }

            Query newQuery = queryFactory.get();
            Query existingQuery = queries.putIfAbsent(newQuery.getQueryId(), newQuery);

            if (existingQuery != null) {
                // Query already registered
                destroyQuietly(newQuery);
                return Optional.of(existingQuery);
            }

            // Only update active queries if this is a new query
            activeQueries.add(newQuery.getQueryId());
            updateIdlenessState();
            return Optional.of(newQuery);
        }

        private synchronized void updateIdlenessState()
        {
            if (activeQueries.isEmpty()) {
                coordinatorIdleStartNanos.compareAndSet(null, System.nanoTime());
                if (drainStarted) {
                    idleAfterDrainingFuture.set(null);
                }
            }
            else {
                verify(!idleAfterDrainingFuture.isDone());
                verify(!drainCompleted);
                coordinatorIdleStartNanos.set(null);
            }
        }

        @Nullable
        public Query getQuery(QueryId queryId)
        {
            return queries.get(queryId);
        }

        public synchronized boolean isDrainStarted()
        {
            return drainStarted;
        }

        public synchronized boolean isDrainCompleted()
        {
            return drainCompleted;
        }

        public synchronized DrainState getDrainState()
        {
            return new DrainState(getDrainStateType(), drainTimeoutTask.map(DrainTimeoutTask::getMaxDrainTime));
        }

        @GuardedBy("this")
        private DrainState.Type getDrainStateType()
        {
            verify(Thread.holdsLock(this));
            if (drainCompleted) {
                return DrainState.Type.DRAINED;
            }
            if (drainStarted) {
                return DrainState.Type.DRAINING;
            }
            return DrainState.Type.UNDRAINED;
        }

        public synchronized boolean trySetDraining(Optional<Duration> maxDrainTime)
        {
            if (drainCompleted) {
                // Already drained
                return false;
            }

            if (!drainStarted) {
                drainStarted = true;
                updateIdlenessState();

                // Schedule a delay of QUERY_LINGER_TIME idle time before drainCompleted
                idleAfterDrainingFuture.addListener(
                        () -> scheduledExecutorService.schedule(
                                this::markDrainingCompleted,
                                Math.max(QUERY_LINGER_TIME.toMillis() - getCoordinatorIdleTime().orElseThrow().toMillis(), 0),
                                TimeUnit.MILLISECONDS),
                        directExecutor());
            }

            updateDrainTimeoutTask(maxDrainTime);
            return true;
        }

        private synchronized void markDrainingCompleted()
        {
            drainCompleted = true;
        }

        @GuardedBy("this")
        private void updateDrainTimeoutTask(Optional<Duration> maxDrainTime)
        {
            verify(Thread.holdsLock(this));

            // Check if update is needed by matching parameters
            if (maxDrainTime.equals(drainTimeoutTask.map(DrainTimeoutTask::getMaxDrainTime))) {
                return;
            }

            drainTimeoutTask.ifPresent(timeoutTask -> timeoutTask.getFuture().cancel(true));

            if (maxDrainTime.isEmpty()) {
                // No timeout requested
                drainTimeoutTask = Optional.empty();
                return;
            }

            long nowNanos = System.nanoTime();
            long startTimeNanos = drainTimeoutTask.map(DrainTimeoutTask::getStartTimeNanos).orElse(nowNanos);
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(nowNanos - startTimeNanos);
            long remainingMillis = Math.max(maxDrainTime.get().toMillis() - elapsedMillis, 0);

            // Schedule a job to fail all queries after the specified timeout
            Future<?> future = scheduledExecutorService.schedule(
                    () -> queries.values().forEach(
                            query -> query.failOrAbandon(new TrinoException(SERVER_SHUTTING_DOWN, "Server is shutting down. Query " + query.getQueryId() + " has been cancelled"))),
                    remainingMillis,
                    TimeUnit.MILLISECONDS);
            drainTimeoutTask = Optional.of(new DrainTimeoutTask(startTimeNanos, maxDrainTime.get(), future));
        }

        public synchronized boolean trySetDrained(Optional<Duration> ifIdleFor, Optional<Instant> clusterStartTime)
        {
            if (drainCompleted) {
                return true;
            }
            Optional<Duration> coordinatorIdleTime = getCoordinatorIdleTime();
            if (coordinatorIdleTime.isEmpty()) {
                return false;
            }
            if (getClusterIdleTime(coordinatorIdleTime.get(), clusterStartTime).compareTo(max(ifIdleFor.orElse(NO_DURATION), QUERY_LINGER_TIME)) < 0) {
                return false;
            }
            drainStarted = true;
            drainCompleted = true;
            updateIdlenessState();
            return true;
        }

        private Optional<Duration> getCoordinatorIdleTime()
        {
            return Optional.ofNullable(coordinatorIdleStartNanos.get())
                    .map(Duration::nanosSince);
        }

        private static Duration getClusterIdleTime(Duration coordinatorIdleTime, Optional<Instant> clusterStartTime)
        {
            if (clusterStartTime.isEmpty()) {
                return coordinatorIdleTime;
            }

            // Clamp idle time to be no more than the cluster age
            long millisSinceStart = Math.max(java.time.Duration.between(clusterStartTime.get(), Instant.now()).toMillis(), 0);
            return min(coordinatorIdleTime, new Duration(millisSinceStart, MILLISECONDS));
        }

        private static class DrainTimeoutTask
        {
            private final long startTimeNanos;
            private final Duration maxDrainTime;
            private final Future<?> future;

            public DrainTimeoutTask(long startTimeNanos, Duration maxDrainTime, Future<?> future)
            {
                this.startTimeNanos = startTimeNanos;
                this.maxDrainTime = requireNonNull(maxDrainTime, "maxDrainTime is null");
                this.future = requireNonNull(future, "future is null");
            }

            public long getStartTimeNanos()
            {
                return startTimeNanos;
            }

            public Duration getMaxDrainTime()
            {
                return maxDrainTime;
            }

            public Future<?> getFuture()
            {
                return future;
            }
        }
    }
}
