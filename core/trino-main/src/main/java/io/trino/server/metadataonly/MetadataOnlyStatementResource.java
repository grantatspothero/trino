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
package io.trino.server.metadataonly;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.airlift.concurrent.SetThreadName;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.starburst.stargate.catalog.QueryCatalog;
import io.starburst.stargate.crypto.SecretSealer;
import io.starburst.stargate.id.AccountId;
import io.starburst.stargate.metadata.StatementRequest;
import io.trino.Session;
import io.trino.client.Column;
import io.trino.client.FailureInfo;
import io.trino.client.QueryData;
import io.trino.client.QueryDataFormats;
import io.trino.client.QueryError;
import io.trino.client.QueryResults;
import io.trino.client.StatementStats;
import io.trino.dispatcher.DispatchManager;
import io.trino.dispatcher.DispatchQuery;
import io.trino.exchange.DirectExchangeInput;
import io.trino.execution.ExecutionFailureInfo;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryManager;
import io.trino.execution.QueryManagerConfig;
import io.trino.execution.QueryState;
import io.trino.execution.buffer.PageDeserializer;
import io.trino.execution.buffer.PagesSerdeFactory;
import io.trino.memory.context.SimpleLocalMemoryContext;
import io.trino.operator.DirectExchangeClient;
import io.trino.operator.DirectExchangeClientSupplier;
import io.trino.server.HttpRequestSessionContextFactory;
import io.trino.server.SessionContext;
import io.trino.server.galaxy.catalogs.DecryptionContextProvider;
import io.trino.server.protocol.QueryResultRows;
import io.trino.server.protocol.Slug;
import io.trino.server.protocol.data.QueryDataProducer;
import io.trino.server.protocol.data.QueryDataProducerFactory;
import io.trino.server.security.InternalPrincipal;
import io.trino.server.security.ResourceSecurity;
import io.trino.spi.ErrorCode;
import io.trino.spi.Page;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.exchange.ExchangeId;
import io.trino.spi.security.Identity;
import io.trino.spi.type.Type;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.Statement;
import io.trino.tracing.TrinoAttributes;
import io.trino.transaction.TransactionId;
import io.trino.util.Failures;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.airlift.concurrent.MoreFutures.whenAnyComplete;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.execution.QueryState.FAILED;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.server.ServletSecurityUtils.authenticatedIdentity;
import static io.trino.server.galaxy.catalogs.SecretDecryption.decryptCatalog;
import static io.trino.server.protocol.ProtocolUtil.createColumns;
import static io.trino.server.protocol.ProtocolUtil.toQueryError;
import static io.trino.server.protocol.QueryResultRows.queryResultRowsBuilder;
import static io.trino.server.security.ResourceSecurity.AccessType.AUTHENTICATED_USER;
import static io.trino.server.security.galaxy.MetadataAccessControllerSupplier.TRANSACTION_ID_KEY;
import static io.trino.spi.StandardErrorCode.EXCEEDED_TIME_LIMIT;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.SERIALIZATION_ERROR;
import static io.trino.sql.SqlFormatter.formatSql;
import static io.trino.tracing.ScopedSpan.scopedSpan;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static jakarta.ws.rs.core.MediaType.TEXT_PLAIN;
import static jakarta.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;
import static jakarta.ws.rs.core.Response.Status.BAD_REQUEST;
import static jakarta.ws.rs.core.Response.Status.FORBIDDEN;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Path("/galaxy/metadata/v1")
public class MetadataOnlyStatementResource
{
    private static final URI INFO_URI = URI.create("info:/");
    private static final SqlParser SQL_PARSER = new SqlParser();

    private final Duration maxWaitTime;

    private final HttpRequestSessionContextFactory sessionContextFactory;
    private final DispatchManager dispatchManager;
    private final Tracer tracer;
    private final QueryManager queryManager;
    private final DirectExchangeClientSupplier directExchangeClientSupplier;
    private final BlockEncodingSerde blockEncodingSerde;
    private final MetadataOnlyTransactionManager transactionManager;
    private final SecretSealer secretSealer;
    private final MetadataOnlySystemState systemState;
    private final DecryptionContextProvider decryptionContextProvider;
    private final QueryDataProducerFactory queryDataProducerFactory;

    @Inject
    public MetadataOnlyStatementResource(
            HttpRequestSessionContextFactory sessionContextFactory,
            DispatchManager dispatchManager,
            Tracer tracer,
            QueryManager queryManager,
            DirectExchangeClientSupplier directExchangeClientSupplier,
            BlockEncodingSerde blockEncodingSerde,
            MetadataOnlyTransactionManager transactionManager,
            SecretSealer secretSealer,
            QueryManagerConfig queryManagerConfig,
            MetadataOnlySystemState systemState,
            DecryptionContextProvider decryptionContextProvider,
            QueryDataProducerFactory queryDataProducerFactory)
    {
        this.sessionContextFactory = requireNonNull(sessionContextFactory, "sessionContextFactory is null");
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.directExchangeClientSupplier = requireNonNull(directExchangeClientSupplier, "directExchangeClientSupplier is null");
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.secretSealer = requireNonNull(secretSealer, "secretSealer is null");
        this.maxWaitTime = queryManagerConfig.getClientTimeout();
        this.systemState = requireNonNull(systemState, "systemState is null");
        this.decryptionContextProvider = requireNonNull(decryptionContextProvider, "decryptionContextProvider is null");
        this.queryDataProducerFactory = requireNonNull(queryDataProducerFactory, "queryDataProducerFactory is null");
    }

    /**
     * Note: this endpoint returns its response in a streaming-supportive manner. The {@code rows} are <em>always</em> output first and are streamed. Therefore,
     * they can be read in a streaming manner. Immediately after the rows, any error is <em>always</em> output. If the field is not present there is no error.
     */
    @ResourceSecurity(AUTHENTICATED_USER)
    @POST
    @Path("statement")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    public QueryResults postStatement(
            StatementRequest request,
            @Context HttpServletRequest servletRequest,
            @Context HttpHeaders httpHeaders)
    {
        if (systemState.isShuttingDown()) {
            throw new WebApplicationException(Response.Status.SERVICE_UNAVAILABLE);
        }

        String statement = request.statement();
        if (isNullOrEmpty(statement)) {
            throw badRequest(BAD_REQUEST, "SQL statement is empty");
        }

        TransactionId transactionId = TransactionId.create();
        Optional<String> remoteAddress = Optional.ofNullable(servletRequest.getRemoteAddr());
        Optional<Identity> identity = authenticatedIdentity(servletRequest)
                .map(i -> Identity.from(i)
                        .withAdditionalExtraCredentials(ImmutableMap.of(TRANSACTION_ID_KEY, transactionId.toString()))
                        .build());
        if (identity.flatMap(Identity::getPrincipal).map(InternalPrincipal.class::isInstance).orElse(false)) {
            throw badRequest(FORBIDDEN, "Internal communication can not be used to start a query");
        }

        MultivaluedMap<String, String> headers = httpHeaders.getRequestHeaders();

        QueryId queryId = dispatchManager.createQueryId();

        SessionContext sessionContext = sessionContextFactory.createSessionContext(headers, remoteAddress, identity);

        systemState.incrementActiveRequests();
        try {
            return executeQuery(queryId, request.accountId(), transactionId, statement, sessionContext, request.catalogs(), request.serviceProperties(), httpHeaders);
        }
        finally {
            systemState.decrementAndGetActiveRequests();
        }
    }

    @ResourceSecurity(AUTHENTICATED_USER)
    @POST
    @Consumes(TEXT_PLAIN)
    @Produces(TEXT_PLAIN)
    @Path("normalizeStatement")
    public String normalizeStatement(
            String requestStatement)
    {
        Statement statement = SQL_PARSER.createStatement(requestStatement, new NodeLocation(1, 1));
        return formatSql(statement).trim();
    }

    private QueryResults executeQuery(QueryId queryId, AccountId accountId, TransactionId transactionId, String statement, SessionContext sessionContext, List<QueryCatalog> catalogs, Map<String, String> serviceProperties, HttpHeaders httpHeaders)
    {
        Span span = tracer.spanBuilder("metadata-query")
                .setAttribute(TrinoAttributes.QUERY_ID, queryId.toString())
                .startSpan();
        try (SetThreadName ignored = new SetThreadName("Resource " + queryId)) {
            UnaryOperator<QueryCatalog> decryptProc = queryCatalog -> {
                Span decryptedCatalogsSpan = tracer.spanBuilder("metadata-decrypt-catalogs")
                        .setAttribute(TrinoAttributes.QUERY_ID, queryId.toString())
                        .startSpan();
                try (var ignore = scopedSpan(decryptedCatalogsSpan)) {
                    return decryptCatalog(secretSealer, accountId, queryCatalog, decryptionContextProvider);
                }
            };

            Span registerQueryCatalogsSpan = tracer.spanBuilder("metadata-register-query-catalogs")
                    .setParent(io.opentelemetry.context.Context.current().with(span))
                    .setAttribute(TrinoAttributes.QUERY_ID, queryId.toString())
                    .startSpan();
            try (var ignore = scopedSpan(registerQueryCatalogsSpan)) {
                transactionManager.registerQueryCatalogs(accountId, sessionContext.getIdentity(), transactionId, queryId, catalogs, serviceProperties, span, decryptProc);
            }
            return executeQuery(statement, span, sessionContext.withTransactionId(transactionId), queryId, httpHeaders);
        }
        catch (Throwable e) {
            dispatchManager.failQuery(queryId, e);
            span.setStatus(StatusCode.ERROR, e.getMessage())
                    .recordException(e)
                    .end();
            return toErrorQueryResult(queryId, e);
        }
        finally {
            transactionManager.destroyQueryCatalogs(transactionId);
            span.end();
        }
    }

    private QueryResults executeQuery(String query, Span span, SessionContext sessionContext, QueryId queryId, HttpHeaders httpHeaders)
    {
        getQueryFuture(dispatchManager.createQuery(queryId, span, Slug.createNew(), sessionContext, query));
        getQueryFuture(dispatchManager.waitForDispatched(queryId));

        DispatchQuery dispatchQuery = dispatchManager.getQuery(queryId);
        if (dispatchQuery.getState().isDone()) {
            span.setStatus(StatusCode.OK);
            return new QueryResults(
                    queryId.toString(),
                    INFO_URI,
                    null,
                    null,
                    null,
                    null,
                    StatementStats.builder()
                            .setState(dispatchQuery.getState().toString())
                            .setRunningPercentage(OptionalDouble.empty())
                            .setProgressPercentage(OptionalDouble.empty())
                            .build(),
                    toQueryError(dispatchQuery.getFullQueryInfo()),
                    ImmutableList.of(),
                    null,
                    null);
        }
        Session session = dispatchQuery.getSession();

        AtomicReference<List<String>> columnNames = new AtomicReference<>();
        AtomicReference<List<Type>> columnTypes = new AtomicReference<>();
        List<Page> pages = new ArrayList<>();

        boolean isDdl = (dispatchQuery.getFullQueryInfo().getUpdateType() != null);
        if (!isDdl) {
            PageDeserializer pageDeserializer = new PagesSerdeFactory(blockEncodingSerde, false).createDeserializer(Optional.empty());
            try (DirectExchangeClient exchangeClient = directExchangeClientSupplier.get(
                    session.getQueryId(),
                    new ExchangeId("direct-exchange-query-results"),
                    span,
                    new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "Query"),
                    queryManager::outputTaskFailed,
                    getRetryPolicy(session))) {
                Object lock = new Object();

                queryManager.setOutputInfoListener(queryId, outputInfo -> {
                    synchronized (lock) {
                        columnNames.compareAndSet(null, outputInfo.getColumnNames());
                        columnTypes.compareAndSet(null, outputInfo.getColumnTypes());

                        outputInfo.getInputs()
                                .stream()
                                .map(exchangeInput -> {
                                    if (exchangeInput instanceof DirectExchangeInput directExchangeInput) {
                                        return directExchangeInput;
                                    }
                                    throw new TrinoException(GENERIC_INTERNAL_ERROR, "SpoolingExchangeInput is not supported");
                                })
                                .forEach(directExchangeInput -> exchangeClient.addLocationIfNotExists(directExchangeInput.getTaskId(), URI.create(directExchangeInput.getLocation())));
                        if (outputInfo.isNoMoreInputs()) {
                            exchangeClient.noMoreLocations();
                        }
                    }
                });

                // read all data from exchange
                for (QueryState state = queryManager.getQueryState(queryId); (state != FAILED) && !exchangeClient.isFinished(); state = queryManager.getQueryState(queryId)) {
                    for (Slice serializedPage = exchangeClient.pollPage(); serializedPage != null; serializedPage = exchangeClient.pollPage()) {
                        Page page = pageDeserializer.deserialize(serializedPage);
                        pages.add(page);
                    }
                    getQueryFuture(whenAnyComplete(ImmutableList.of(queryManager.getStateChange(queryId, state), exchangeClient.isBlocked())));
                }
            }
        }

        queryManager.resultsConsumed(queryId);

        // wait for query to finish
        for (QueryState queryState = queryManager.getQueryState(queryId); !queryState.isDone(); queryState = queryManager.getQueryState(queryId)) {
            getQueryFuture(queryManager.getStateChange(queryId, queryState));
        }

        QueryInfo queryInfo = dispatchQuery.getFullQueryInfo();

        List<Column> columns = null;
        QueryResultRows resultRows = QueryResultRows.empty(session);
        Long updateCount = null;
        if (queryInfo.getState() != FAILED) {
            QueryResultRows.Builder builder = queryResultRowsBuilder(session);
            if (!pages.isEmpty()) {
                columns = createColumns(columnNames.get(), columnTypes.get(), true);
                builder.withColumnsAndTypes(columns, columnTypes.get()).addPages(pages);
            }

            if (isDdl) {
                QueryResultRows tempRows = builder.build();
                updateCount = tempRows.getUpdateCount().orElse(null);
            }
            else {
                resultRows = builder.build();
                if (columns == null) {
                    columns = ImmutableList.of();
                }
            }
        }

        Set<String> supportedDataFormats = QueryDataFormats.fromHeaderValue(httpHeaders.getHeaderString(session.getProtocolHeaders().requestSupportedQueryDataFormats()));
        QueryDataProducer queryDataProducer = queryDataProducerFactory.create(session, queryId, supportedDataFormats);
        QueryData queryData = queryDataProducer.produce(session, resultRows, true, throwable -> {
            throw new TrinoException(SERIALIZATION_ERROR, "Error converting output to client protocol", throwable);
        });

        span.setStatus(StatusCode.OK);
        return new QueryResults(
                queryId.toString(),
                INFO_URI,
                null,
                null,
                columns,
                queryData,
                StatementStats.builder()
                        .setState(queryInfo.getState().toString())
                        .setRunningPercentage(OptionalDouble.empty())
                        .setProgressPercentage(OptionalDouble.empty())
                        .build(),
                toQueryError(queryInfo),
                ImmutableList.of(),
                queryInfo.getUpdateType(),
                updateCount);
    }

    private <T> void getQueryFuture(ListenableFuture<T> future)
    {
        try {
            future.get(maxWaitTime.toMillis(), MILLISECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Thread interrupted", e);
        }
        catch (ExecutionException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error processing query", e.getCause());
        }
        catch (TimeoutException e) {
            throw new TrinoException(EXCEEDED_TIME_LIMIT, "Query ran longer than " + maxWaitTime);
        }
    }

    private static QueryResults toErrorQueryResult(QueryId queryId, Throwable e)
    {
        ExecutionFailureInfo executionFailure = Failures.toFailure(e);
        FailureInfo failure = executionFailure.toFailureInfo();
        ErrorCode errorCode = firstNonNull(executionFailure.getErrorCode(), GENERIC_INTERNAL_ERROR.toErrorCode());
        QueryError queryError = new QueryError(
                firstNonNull(failure.getMessage(), "Internal error"),
                null,
                errorCode.getCode(),
                errorCode.getName(),
                errorCode.getType().toString(),
                failure.getErrorLocation(),
                failure);

        return new QueryResults(
                queryId.toString(),
                INFO_URI,
                null,
                null,
                null,
                null,
                StatementStats.builder()
                        .setState(FAILED.toString())
                        .setRunningPercentage(OptionalDouble.empty())
                        .setProgressPercentage(OptionalDouble.empty())
                        .build(),
                queryError,
                ImmutableList.of(),
                null,
                null);
    }

    private static WebApplicationException badRequest(Response.Status status, String message)
    {
        throw new WebApplicationException(
                Response.status(status)
                        .type(TEXT_PLAIN_TYPE)
                        .entity(message)
                        .build());
    }
}