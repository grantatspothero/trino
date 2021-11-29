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
package io.trino.plugin.clickhouse;

import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.ForwardingConnection;
import io.trino.spi.connector.ConnectorSession;

import javax.annotation.PreDestroy;

import java.sql.Connection;
import java.sql.SQLException;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class ClickHouseConnectionFactory
        implements ConnectionFactory
{
    private final ConnectionFactory delegate;

    public ClickHouseConnectionFactory(ConnectionFactory delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public Connection openConnection(ConnectorSession session)
            throws SQLException
    {
        return new ForwardingConnection()
        {
            private final Connection delegate = ClickHouseConnectionFactory.this.delegate.openConnection(session);

            @Override
            protected Connection getDelegate()
            {
                return delegate;
            }

            @Override
            public void setAutoCommit(boolean autoCommit)
                    throws SQLException
            {
                // Clickhouse "Transactions are not supported"
            }
            @Override
            public void commit() throws SQLException {
                // Clickhouse "Transactions are not supported"
            }

            @Override
            public void rollback() throws SQLException {
                // Clickhouse "Transactions are not supported"
            }
        };
    }

    @Override
    @PreDestroy
    public void close()
            throws SQLException
    {
        delegate.close();
    }
}
