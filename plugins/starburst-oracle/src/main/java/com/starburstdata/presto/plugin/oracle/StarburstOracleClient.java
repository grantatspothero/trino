/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.oracle;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.starburstdata.presto.license.LicenseManager;
import com.starburstdata.presto.plugin.jdbc.stats.JdbcStatisticsConfig;
import com.starburstdata.presto.plugin.jdbc.stats.TableStatisticsClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcExpression;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcMetadataConfig;
import io.prestosql.plugin.jdbc.JdbcSplit;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.plugin.jdbc.QueryBuilder;
import io.prestosql.plugin.jdbc.RemoteTableName;
import io.prestosql.plugin.jdbc.expression.AggregateFunctionRewriter;
import io.prestosql.plugin.jdbc.expression.AggregateFunctionRule;
import io.prestosql.plugin.jdbc.expression.ImplementAvgDecimal;
import io.prestosql.plugin.jdbc.expression.ImplementAvgFloatingPoint;
import io.prestosql.plugin.jdbc.expression.ImplementCount;
import io.prestosql.plugin.jdbc.expression.ImplementCountAll;
import io.prestosql.plugin.jdbc.expression.ImplementMinMax;
import io.prestosql.plugin.jdbc.expression.ImplementSum;
import io.prestosql.plugin.oracle.OracleClient;
import io.prestosql.plugin.oracle.OracleConfig;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.AggregateFunction;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.statistics.ColumnStatistics;
import io.prestosql.spi.statistics.Estimate;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.spi.type.DecimalType;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleTypes;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.starburstdata.presto.license.StarburstPrestoFeature.ORACLE_EXTENSIONS;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_NON_TRANSIENT_ERROR;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;

public class StarburstOracleClient
        extends OracleClient
{
    private static final int DEFAULT_ROW_FETCH_SIZE = 1000;
    private static final int PRESTO_BIGINT_TYPE = 832_424_001;

    private final boolean synonymsEnabled;
    private final LicenseManager licenseManager;
    private final AggregateFunctionRewriter aggregateFunctionRewriter;
    private final TableStatisticsClient tableStatisticsClient;

    @Inject
    public StarburstOracleClient(
            LicenseManager licenseManager,
            BaseJdbcConfig config,
            JdbcMetadataConfig jdbcMetadataConfig,
            JdbcStatisticsConfig statisticsConfig,
            OracleConfig oracleConfig,
            ConnectionFactory connectionFactory)
    {
        super(config, oracleConfig, connectionFactory);
        synonymsEnabled = oracleConfig.isSynonymsEnabled();
        this.licenseManager = requireNonNull(licenseManager, "licenseManager is null");
        JdbcTypeHandle bigintTypeHandle = new JdbcTypeHandle(PRESTO_BIGINT_TYPE, Optional.empty(), 0, 0, Optional.empty());
        this.aggregateFunctionRewriter = new AggregateFunctionRewriter(
                this::quoted,
                ImmutableSet.<AggregateFunctionRule>builder()
                        .add(new ImplementCountAll(bigintTypeHandle))
                        .add(new ImplementCount(bigintTypeHandle))
                        .add(new ImplementMinMax())
                        .add(new ImplementSum(StarburstOracleClient::toTypeHandle))
                        .add(new ImplementAvgFloatingPoint())
                        .add(new ImplementAvgDecimal())
                        .add(new ImplementOracleStddev())
                        .add(new ImplementOracleStddevPop())
                        .add(new ImplementOracleVariance())
                        .add(new ImplementOracleVariancePop())
                        .build());
        tableStatisticsClient = new TableStatisticsClient(this::readTableStatistics, statisticsConfig);

        if (jdbcMetadataConfig.isAggregationPushdownEnabled()) {
            licenseManager.checkFeature(ORACLE_EXTENSIONS);
        }
    }

    /** @deprecated This should never be called; use {@link OracleSplitManager} to get splits. */
    @Deprecated
    @Override
    public ConnectorSplitSource getSplits(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        throw new UnsupportedOperationException("Wrong entry point to get Oracle splits");
    }

    @Override
    public PreparedStatement buildSql(ConnectorSession session, Connection connection, JdbcSplit split, JdbcTableHandle table, List<JdbcColumnHandle> columns)
            throws SQLException
    {
        return new OracleQueryBuilder(this, (OracleSplit) split).buildSql(
                session,
                connection,
                table.getRemoteTableName(),
                table.getGroupingSets(),
                columns,
                table.getConstraint(),
                split.getAdditionalPredicate(),
                tryApplyLimit(table.getLimit()));
    }

    @Override
    protected String generateTemporaryTableName()
    {
        // Oracle before 12.2 doesn't allow identifiers over 30 characters
        String id = super.generateTemporaryTableName();
        return id.substring(0, min(30, id.length()));
    }

    @Override
    public String quoted(String name)
    {
        if (name.contains("\"")) {
            // ORA-03001: unimplemented feature
            throw new PrestoException(JDBC_NON_TRANSIENT_ERROR, "Oracle does not support escaping '\"' in identifiers");
        }
        return identifierQuote + name + identifierQuote;
    }

    @Override
    public OracleConnection getConnection(JdbcIdentity identity, JdbcSplit split)
            throws SQLException
    {
        OracleConnection connection = (OracleConnection) super.getConnection(identity, split);
        try {
            connection.setDefaultRowPrefetch(DEFAULT_ROW_FETCH_SIZE);
        }
        catch (SQLException e) {
            connection.close();
            throw e;
        }
        return connection;
    }

    @Override
    protected void renameTable(JdbcIdentity identity, String catalogName, String schemaName, String tableName, SchemaTableName newTable)
    {
        if (!schemaName.equals(newTable.getSchemaName().toUpperCase(ENGLISH))) {
            throw new PrestoException(NOT_SUPPORTED, "Table rename across schemas is not supported");
        }

        super.renameTable(identity, catalogName, schemaName, tableName, newTable);
    }

    @Override
    // TODO: migrate to OSS?
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        if (!synonymsEnabled) {
            return super.getColumns(session, tableHandle);
        }
        // MAJOR HACK ALERT!!!
        // We had to introduce the hack because of bug in Oracle JDBC client where
        // BaseJdbcClient#getColumns is not working when openProxySession is used and setIncludeSynonym(true) are used.
        // Below we are forcing to use oracle.jdbc.driver.OracleDatabaseMetaData.getColumnsWithWildcardsPlsql,
        // this method was used when setIncludeSynonym(false) is set, then openProxySession is also working as expected
        // Forcing is done by using wildcard '%' at the end of table name. And so we have to filter rows with columns from other tables.
        // Whenever you change this method make sure TestOracleIntegrationSmokeTest.testGetColumns covers your changes.
        try (Connection connection = connectionFactory.openConnection(JdbcIdentity.from(session))) {
            try (ResultSet resultSet = getColumns(tableHandle, connection.getMetaData(), "%")) {
                List<JdbcColumnHandle> columns = new ArrayList<>();
                while (resultSet.next()) {
                    if (!resultSet.getString("TABLE_NAME").equals(tableHandle.getTableName())) {
                        continue;
                    }
                    JdbcTypeHandle typeHandle = new JdbcTypeHandle(
                            resultSet.getInt("DATA_TYPE"),
                            Optional.ofNullable(resultSet.getString("TYPE_NAME")),
                            resultSet.getInt("COLUMN_SIZE"),
                            resultSet.getInt("DECIMAL_DIGITS"),
                            Optional.empty());
                    Optional<ColumnMapping> columnMapping = toPrestoType(session, connection, typeHandle);
                    // skip unsupported column types
                    if (columnMapping.isPresent()) {
                        String columnName = resultSet.getString("COLUMN_NAME");
                        columns.add(new JdbcColumnHandle(columnName, typeHandle, columnMapping.get().getType()));
                    }
                }
                if (columns.isEmpty()) {
                    // Table has no supported columns, but such table is not supported in Presto
                    throw new TableNotFoundException(tableHandle.getSchemaTableName());
                }
                return ImmutableList.copyOf(columns);
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    private static ResultSet getColumns(JdbcTableHandle tableHandle, DatabaseMetaData metadata, String tableNameSuffix)
            throws SQLException
    {
        String escape = metadata.getSearchStringEscape();
        return metadata.getColumns(
                tableHandle.getCatalogName(),
                escapeNamePattern(Optional.ofNullable(tableHandle.getSchemaName()), escape).orElse(null),
                escapeNamePattern(Optional.ofNullable(tableHandle.getTableName()), escape).orElse("") + tableNameSuffix,
                null);
    }

    @Override
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, Connection connection, JdbcTypeHandle type)
    {
        if (type.getJdbcType() == PRESTO_BIGINT_TYPE) {
            // Synthetic column
            return Optional.of(bigintColumnMapping());
        }

        Optional<ColumnMapping> mappingToVarchar = getForcedMappingToVarchar(type);
        if (mappingToVarchar.isPresent()) {
            return mappingToVarchar;
        }

        return super.toPrestoType(session, connection, type);
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        licenseManager.checkFeature(ORACLE_EXTENSIONS);
        // TODO support complex ConnectorExpressions
        return aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
    }

    private static Optional<JdbcTypeHandle> toTypeHandle(DecimalType decimalType)
    {
        return Optional.of(new JdbcTypeHandle(OracleTypes.NUMBER, Optional.empty(), decimalType.getPrecision(), decimalType.getScale(), Optional.empty()));
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> format("SELECT * FROM (%s) WHERE ROWNUM <= %s", sql, limit));
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        return StarburstOracleSessionProperties.getParallelismType(session) == OracleParallelismType.NO_PARALLELISM;
    }

    @Override
    // TODO: migrate to OSS?
    public void dropSchema(JdbcIdentity identity, String schemaName)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support dropping schemas");
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle, TupleDomain<ColumnHandle> tupleDomain)
    {
        return tableStatisticsClient.getTableStatistics(session, handle, tupleDomain);
    }

    private Optional<TableStatistics> readTableStatistics(ConnectorSession session, JdbcTableHandle table)
            throws SQLException
    {
        if (table.getGroupingSets().isPresent()) {
            // TODO retrieve statistics for base table and derive statistics for the aggregation
            return Optional.empty();
        }

        try (Connection connection = connectionFactory.openConnection(JdbcIdentity.from(session));
                Handle handle = Jdbi.open(connection)) {
            StatisticsDao statisticsDao = new StatisticsDao(handle);

            Long rowCount = statisticsDao.getRowCount(table.getSchemaName(), table.getTableName());
            if (rowCount == null) {
                return Optional.empty();
            }

            TableStatistics.Builder tableStatistics = TableStatistics.builder();
            tableStatistics.setRowCount(Estimate.of(rowCount));

            if (rowCount == 0) {
                return Optional.of(tableStatistics.build());
            }

            Map<String, ColumnStatisticsResult> columnStatistics = statisticsDao.getColumnStatistics(table.getSchemaName(), table.getTableName()).stream()
                    .collect(toImmutableMap(ColumnStatisticsResult::getColumnName, identity()));

            for (JdbcColumnHandle column : this.getColumns(session, table)) {
                ColumnStatisticsResult result = columnStatistics.get(column.getColumnName());
                if (result == null) {
                    continue;
                }

                ColumnStatistics statistics = ColumnStatistics.builder()
                        .setNullsFraction(result.getNullsCount()
                                .map(nullsCount -> Estimate.of(1.0 * nullsCount / rowCount))
                                .orElseGet(Estimate::unknown))
                        .setDistinctValuesCount(result.getDistinctValuesCount()
                                .map(Estimate::of)
                                .orElseGet(Estimate::unknown))
                        .setDataSize(result.getAverageColumnLength()
                                /*
                                 * ALL_TAB_COLUMNS.AVG_COL_LEN is hard to interpret precisely:
                                 * - it can be `0` for all-null column
                                 * - it can be `len+1` for varchar column filled with constant of length `len`, as if each row contained a is-null byte or length
                                 * - it can be `len/2+1` for varchar column half-filled with constant (or random) of length `len`, as if each row contained a is-null byte or length
                                 * - it can be `2` for varchar column with single non-null value of length 10, as if ... (?)
                                 * - it looks storage size does not directly depend on `IS NULL` column attribute
                                 *
                                 * Since the interpretation of the value is not obvious, we do not deduce is-null bytes. They will be accounted for second time in
                                 * `PlanNodeStatsEstimate.getOutputSizeForSymbol`, but this is the safer thing to do.
                                 */
                                .map(averageColumnLength -> Estimate.of(1.0 * averageColumnLength * rowCount))
                                .orElseGet(Estimate::unknown))
                        .build();

                tableStatistics.setColumnStatistics(column, statistics);
            }

            return Optional.of(tableStatistics.build());
        }
    }

    private static class StatisticsDao
    {
        private final Handle handle;

        public StatisticsDao(Handle handle)
        {
            this.handle = requireNonNull(handle, "handle is null");
        }

        Long getRowCount(String schema, String tableName)
        {
            return handle.createQuery("SELECT NUM_ROWS FROM ALL_TAB_STATISTICS WHERE OWNER = :schema AND TABLE_NAME = :table_name and PARTITION_NAME IS NULL")
                    .bind("schema", schema)
                    .bind("table_name", tableName)
                    .mapTo(Long.class)
                    .findFirst()
                    .orElse(null);
        }

        List<ColumnStatisticsResult> getColumnStatistics(String schema, String tableName)
        {
            // [PRESTO-3425] we are not using ALL_TAB_COL_STATISTICS, here because we observed queries which took multiple minutes when obtaining statistics for partitioned tables.
            //               It adds slight risk, because the statistics-related columns in ALL_TAB_COLUMNS are marked as deprecated and present only for backward
            //               compatibility with Oracle 7 (see: https://docs.oracle.com/cd/B14117_01/server.101/b10755/statviews_1180.htm)
            return handle.createQuery("SELECT COLUMN_NAME, NUM_NULLS, NUM_DISTINCT, AVG_COL_LEN FROM ALL_TAB_COLUMNS WHERE OWNER = :schema AND TABLE_NAME = :table_name")
                    .bind("schema", schema)
                    .bind("table_name", tableName)
                    .map((rs, ctx) -> new ColumnStatisticsResult(
                            requireNonNull(rs.getString("COLUMN_NAME"), "COLUMN_NAME is null"),
                            Optional.ofNullable(rs.getObject("NUM_NULLS", Long.class)),
                            Optional.ofNullable(rs.getObject("NUM_DISTINCT", Long.class)),
                            Optional.ofNullable(rs.getObject("AVG_COL_LEN", Long.class))))
                    .list();
        }
    }

    private static class ColumnStatisticsResult
    {
        private final String columnName;
        private final Optional<Long> nullsCount;
        private final Optional<Long> distinctValuesCount;
        private final Optional<Long> averageColumnLength;

        ColumnStatisticsResult(String columnName, Optional<Long> nullsCount, Optional<Long> distinctValuesCount, Optional<Long> averageColumnLength)
        {
            this.columnName = columnName;
            this.nullsCount = nullsCount;
            this.distinctValuesCount = distinctValuesCount;
            this.averageColumnLength = averageColumnLength;
        }

        String getColumnName()
        {
            return columnName;
        }

        Optional<Long> getNullsCount()
        {
            return nullsCount;
        }

        Optional<Long> getDistinctValuesCount()
        {
            return distinctValuesCount;
        }

        Optional<Long> getAverageColumnLength()
        {
            return averageColumnLength;
        }
    }

    private static class OracleQueryBuilder
            extends QueryBuilder
    {
        private final OracleSplit split;

        public OracleQueryBuilder(JdbcClient jdbcClient, OracleSplit split)
        {
            super(jdbcClient);
            this.split = requireNonNull(split, "split is null");
        }

        @Override
        protected String getRelation(RemoteTableName remoteTableName)
        {
            String tableName = super.getRelation(remoteTableName);
            return split.getPartitionNames()
                    .map(batch -> batch.stream()
                            .map(partitionName -> format("SELECT * FROM %s PARTITION (%s)", tableName, partitionName))
                            .collect(joining(" UNION ALL ", "(", ")"))) // wrap subquery in parentheses
                    .orElse(tableName);
        }
    }
}