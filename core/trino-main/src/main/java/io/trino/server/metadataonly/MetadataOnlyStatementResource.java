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
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.SetThreadName;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.starburst.stargate.crypto.SecretEncryptionContext;
import io.starburst.stargate.crypto.SecretSealer;
import io.starburst.stargate.crypto.SecretSealer.SealedSecret;
import io.starburst.stargate.id.AccountId;
import io.starburst.stargate.id.TrinoPlaneId;
import io.starburst.stargate.metadata.EncryptedSecret;
import io.starburst.stargate.metadata.QueryCatalog;
import io.starburst.stargate.metadata.StatementRequest;
import io.trino.Session;
import io.trino.client.Column;
import io.trino.client.FailureInfo;
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
import io.trino.server.protocol.QueryResultRows;
import io.trino.server.protocol.Slug;
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
import io.trino.tracing.TrinoAttributes;
import io.trino.transaction.TransactionId;
import io.trino.util.Failures;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Maps.transformValues;
import static io.airlift.concurrent.MoreFutures.whenAnyComplete;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.execution.QueryState.FAILED;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.server.HttpRequestSessionContextFactory.AUTHENTICATED_IDENTITY;
import static io.trino.server.protocol.ProtocolUtil.createColumns;
import static io.trino.server.protocol.ProtocolUtil.toQueryError;
import static io.trino.server.protocol.QueryResultRows.queryResultRowsBuilder;
import static io.trino.server.security.ResourceSecurity.AccessType.AUTHENTICATED_USER;
import static io.trino.server.security.galaxy.MetadataAccessControllerSupplier.TRANSACTION_ID_KEY;
import static io.trino.spi.StandardErrorCode.EXCEEDED_TIME_LIMIT;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.SERIALIZATION_ERROR;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.FORBIDDEN;

@Path("/galaxy/metadata/v1/statement")
public class MetadataOnlyStatementResource
{
    private static final URI INFO_URI = URI.create("info:/");
    private final Duration maxWaitTime;

    private final HttpRequestSessionContextFactory sessionContextFactory;
    private final DispatchManager dispatchManager;
    private final Tracer tracer;
    private final QueryManager queryManager;
    private final DirectExchangeClientSupplier directExchangeClientSupplier;
    private final BlockEncodingSerde blockEncodingSerde;
    private final MetadataOnlyTransactionManager transactionManager;
    private final SecretSealer secretSealer;
    private final TrinoPlaneId trinoPlaneId;
    private final MetadataOnlySystemState systemState;

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
            MetadataOnlyConfig config,
            QueryManagerConfig queryManagerConfig,
            MetadataOnlySystemState systemState)
    {
        this.sessionContextFactory = requireNonNull(sessionContextFactory, "sessionContextFactory is null");
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.directExchangeClientSupplier = requireNonNull(directExchangeClientSupplier, "directExchangeClientSupplier is null");
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.secretSealer = requireNonNull(secretSealer, "secretSealer is null");
        trinoPlaneId = config.getTrinoPlaneId();
        this.maxWaitTime = queryManagerConfig.getClientTimeout();
        this.systemState = requireNonNull(systemState, "systemState is null");
    }

    /**
     * Note: this endpoint returns its response in a streaming-supportive manner. The {@code rows} are <em>always</em> output first and are streamed. Therefore,
     * they can be read in a streaming manner. Immediately after the rows, any error is <em>always</em> output. If the field is not present there is no error.
     */
    @ResourceSecurity(AUTHENTICATED_USER)
    @POST
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    public QueryResults postStatement(
            StatementRequest request,
            @Context HttpServletRequest servletRequest,
            @Context HttpHeaders httpHeaders,
            @Context UriInfo uriInfo)
    {
        if (systemState.isShuttingDown()) {
            throw new WebApplicationException(Status.SERVICE_UNAVAILABLE);
        }

        String statement = request.statement();
        if (isNullOrEmpty(statement)) {
            throw badRequest(BAD_REQUEST, "SQL statement is empty");
        }

        TransactionId transactionId = TransactionId.create();
        Optional<String> remoteAddress = Optional.ofNullable(servletRequest.getRemoteAddr());
        Optional<Identity> identity = Optional.ofNullable((Identity) servletRequest.getAttribute(AUTHENTICATED_IDENTITY))
                .map(i -> Identity.from(i)
                        .withAdditionalExtraCredentials(ImmutableMap.of(TRANSACTION_ID_KEY, transactionId.toString()))
                        .build());
        if (identity.flatMap(Identity::getPrincipal).map(InternalPrincipal.class::isInstance).orElse(false)) {
            throw badRequest(FORBIDDEN, "Internal communication can not be used to start a query");
        }

        MultivaluedMap<String, String> headers = httpHeaders.getRequestHeaders();

        SessionContext sessionContext = sessionContextFactory.createSessionContext(headers, Optional.empty(), remoteAddress, identity);
        List<QueryCatalog> decryptedCatalogs = request.catalogs().stream().map(queryCatalog -> decryptCatalog(request.accountId(), queryCatalog)).collect(toImmutableList());

        systemState.incrementActiveRequests();
        try {
            return executeQuery(request.accountId(), transactionId, statement, sessionContext, decryptedCatalogs, request.serviceProperties());
        }
        finally {
            systemState.decrementAndGetActiveRequests();
        }
    }

    private QueryResults executeQuery(AccountId accountId, TransactionId transactionId, String statement, SessionContext sessionContext, List<QueryCatalog> catalogs, Map<String, String> serviceProperties)
    {
        QueryId queryId = dispatchManager.createQueryId();
        Span span = tracer.spanBuilder("metadata-query")
                .setAttribute(TrinoAttributes.QUERY_ID, queryId.toString())
                .startSpan();
        try (SetThreadName ignored = new SetThreadName("Resource " + queryId)) {
            transactionId = transactionManager.registerQueryCatalogs(accountId, sessionContext.getIdentity(), transactionId, queryId, catalogs, serviceProperties);
            return executeQuery(statement, span, sessionContext.withTransactionId(transactionId), queryId);
        }
        catch (Throwable e) {
            dispatchManager.failQuery(queryId, e);
            span.setStatus(StatusCode.ERROR, e.getMessage())
                    .recordException(e)
                    .end();
            return toErrorQueryResult(queryId, e);
        }
        finally {
            if (transactionId != null) {
                transactionManager.destroyQueryCatalogs(transactionId);
            }
            span.end();
        }
    }

    private QueryResults executeQuery(String query, Span span, SessionContext sessionContext, QueryId queryId)
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
                    new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "Query"),
                    queryManager::outputTaskFailed,
                    getRetryPolicy(session))) {
                queryManager.setOutputInfoListener(queryId, outputInfo -> {
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
        QueryResultRows resultRows = null;
        Long updateCount = null;
        if (queryInfo.getState() != FAILED) {
            QueryResultRows.Builder builder = queryResultRowsBuilder(session)
                    .withExceptionConsumer(throwable -> {
                        throw new TrinoException(SERIALIZATION_ERROR, "Error converting output to client protocol", throwable);
                    });
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

        span.setStatus(StatusCode.OK);
        return new QueryResults(
                queryId.toString(),
                INFO_URI,
                null,
                null,
                columns,
                resultRows,
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

    private static WebApplicationException badRequest(Status status, String message)
    {
        throw new WebApplicationException(
                Response.status(status)
                        .type(TEXT_PLAIN_TYPE)
                        .entity(message)
                        .build());
    }

    private QueryCatalog decryptCatalog(AccountId accountId, QueryCatalog queryCatalog)
    {
        Map<String, String> decryptedProperties = decryptSecrets(accountId, queryCatalog);
        return new QueryCatalog(queryCatalog.catalogName(), queryCatalog.connectorName(), decryptedProperties, queryCatalog.secretsMap(), queryCatalog.secrets());
    }

    private Map<String, String> decryptSecrets(AccountId accountId, QueryCatalog queryCatalog)
    {
        return ImmutableMap.copyOf(transformValues(queryCatalog.properties(), propertyValue -> {
            String decryptedPropertyValue = propertyValue;
            for (EncryptedSecret secret : queryCatalog.secrets().orElseGet(ImmutableSet::of)) {
                if (decryptedPropertyValue.contains(secret.placeholderText())) {
                    Map<String, String> metadataEncryptionContext = SecretEncryptionContext.forVerifier(accountId, trinoPlaneId, Optional.of(queryCatalog.catalogName()), secret.secretName());
                    String decryptedValue = secretSealer.unsealSecret(SealedSecret.fromString(secret.encryptedValue()), metadataEncryptionContext);
                    decryptedPropertyValue = decryptedPropertyValue.replace(secret.placeholderText(), decryptedValue);
                }
            }
            return decryptedPropertyValue;
        }));
    }
}
