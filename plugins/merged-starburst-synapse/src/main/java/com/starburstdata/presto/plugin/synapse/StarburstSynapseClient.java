/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.synapse;

import com.starburstdata.presto.plugin.jdbc.redirection.TableScanRedirection;
import com.starburstdata.presto.plugin.jdbc.stats.JdbcStatisticsConfig;
import com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SortItem;
import io.trino.spi.type.CharType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.Types;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timeColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.TimeType.TIME;
import static java.lang.String.format;

public class StarburstSynapseClient
        extends StarburstSqlServerClient
{
    private static final int MAX_NVARCHAR_LENGTH = 4000;
    private static final int MAX_NCHAR_LENGTH = 4000;
    private static final int MAX_VARBINARY_LENGTH = 8000;

    @Inject
    public StarburstSynapseClient(BaseJdbcConfig config, JdbcStatisticsConfig statisticsConfig, TableScanRedirection tableScanRedirection, ConnectionFactory connectionFactory)
    {
        super(config, statisticsConfig, tableScanRedirection, connectionFactory);
    }

    @Override
    protected void renameTable(ConnectorSession session, String catalogName, String schemaName, String tableName, SchemaTableName newTable)
    {
        if (!schemaName.equals(newTable.getSchemaName())) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming tables across schemas");
        }
        String sql = format(
                "RENAME OBJECT %s TO %s",
                quoted(catalogName, schemaName, tableName),
                newTable.getTableName());
        execute(session, sql);
    }

    // TODO(https://starburstdata.atlassian.net/browse/PRESTO-5074) Add support for DATETIME2 mapping to Synapse Connector

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        switch (typeHandle.getJdbcType()) {
            case Types.TIME:
                // SQL Server time read mapping added in https://github.com/trinodb/trino/pull/7122 does not work for Synapse
                // TODO (https://starburstdata.atlassian.net/browse/PRESTO-5703) Support TIME(p) in Synapse
                // TODO (https://starburstdata.atlassian.net/browse/PRESTO-5693) add type mapping tests for Synapse
                return Optional.of(timeColumnMapping(TIME));
        }

        return super.toColumnMapping(session, connection, typeHandle);
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type instanceof VarcharType) {
            VarcharType varcharType = (VarcharType) type;
            String dataType;
            if (varcharType.isUnbounded()) {
                dataType = "nvarchar(" + MAX_NVARCHAR_LENGTH + ")";
            }
            else if (varcharType.getBoundedLength() > MAX_NVARCHAR_LENGTH) {
                throw new TrinoException(StandardErrorCode.NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
            }
            else {
                dataType = "nvarchar(" + varcharType.getBoundedLength() + ")";
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }

        if (type instanceof CharType) {
            CharType charType = (CharType) type;
            String dataType;
            if (charType.getLength() > MAX_NCHAR_LENGTH) {
                throw new TrinoException(StandardErrorCode.NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
            }
            else {
                dataType = "nchar(" + charType.getLength() + ")";
            }
            return WriteMapping.sliceMapping(dataType, charWriteFunction());
        }

        if (type instanceof VarbinaryType) {
            return WriteMapping.sliceMapping("varbinary(" + MAX_VARBINARY_LENGTH + ")", varbinaryWriteFunction());
        }

        if (type instanceof TimeType) {
            // SQL Server time write mapping added in https://github.com/trinodb/trino/pull/7122 does not work for Synapse
            // TODO (https://starburstdata.atlassian.net/browse/PRESTO-5703) Support TIME(p) in Synapse
            throw new TrinoException(StandardErrorCode.NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
        }

        return super.toWriteMapping(session, type);
    }

    @Override
    public boolean supportsTopN(ConnectorSession session, JdbcTableHandle handle, List<SortItem> sortOrder)
    {
        // TODO: Synapse doesn't support the SQL Server syntax OFFSET ... FETCH (https://starburstdata.atlassian.net/browse/PRESTO-5683)
        return false;
    }

    @Override
    protected Optional<TopNFunction> topNFunction()
    {
        // TODO: Synapse doesn't support the SQL Server syntax OFFSET ... FETCH (https://starburstdata.atlassian.net/browse/PRESTO-5683)
        return Optional.empty();
    }
}