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
package io.trino.plugin.kudu;

import io.trino.plugin.base.authentication.CachingKerberosAuthentication;
import org.apache.kudu.Schema;
import org.apache.kudu.client.AlterTableOptions;
import org.apache.kudu.client.AlterTableResponse;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.DeleteTableResponse;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.ListTablesResponse;

import java.io.IOException;

public class KerberizedKuduClient
        implements IKuduClient
{
    KuduClient client;
    CachingKerberosAuthentication cachingKerberosAuthentication;

    KerberizedKuduClient(KuduClient client, CachingKerberosAuthentication cachingKerberosAuthentication)
    {
        this.client = client;
        this.cachingKerberosAuthentication = cachingKerberosAuthentication;
    }

    public KuduTable createTable(String name, Schema schema, CreateTableOptions builder)
            throws KuduException
    {
        return maybeRefreshKerberosTicketAndRun(() -> client.createTable(name, schema, builder));
    }

    public DeleteTableResponse deleteTable(String name)
            throws KuduException
    {
        return maybeRefreshKerberosTicketAndRun(() -> client.deleteTable(name));
    }

    public AlterTableResponse alterTable(String name, AlterTableOptions ato)
            throws KuduException
    {
        return maybeRefreshKerberosTicketAndRun(() -> client.alterTable(name, ato));
    }

    public ListTablesResponse getTablesList()
            throws KuduException
    {
        return maybeRefreshKerberosTicketAndRun(() -> client.getTablesList());
    }

    public ListTablesResponse getTablesList(String nameFilter)
            throws KuduException
    {
        return maybeRefreshKerberosTicketAndRun(() -> client.getTablesList(nameFilter));
    }

    public boolean tableExists(String name)
            throws KuduException
    {
        return maybeRefreshKerberosTicketAndRun(() -> client.tableExists(name));
    }

    public KuduTable openTable(final String name)
            throws KuduException
    {
        return maybeRefreshKerberosTicketAndRun(() -> client.openTable(name));
    }

    // Fully local operation with no RPCs, no need to wrap
    public KuduSession newSession()
    {
        return client.newSession();
    }

    public KuduScanner.KuduScannerBuilder newScannerBuilder(KuduTable table)
    {
        return client.newScannerBuilder(table);
    }

    public KuduScanToken.KuduScanTokenBuilder newScanTokenBuilder(KuduTable table)
    {
        return client.newScanTokenBuilder(table);
    }

    @Override
    public void close()
            throws KuduException
    {
        maybeRefreshKerberosTicketAndRun(() -> {
            client.close();
            return null;
        });
    }

    @Override
    public KuduScanner deserializeIntoScanner(byte[] serializedScanToken)
            throws IOException
    {
        return maybeRefreshKerberosTicketAndRun(() ->
                KuduScanToken.deserializeIntoScanner(serializedScanToken, client));
    }

    private <R, E extends Exception> R maybeRefreshKerberosTicketAndRun(CheckedSupplier<R, E> supplier) throws E
    {
        cachingKerberosAuthentication.reauthenticateIfSoonWillBeExpired();
        return supplier.get();
    }

    @FunctionalInterface
    private interface CheckedSupplier<T, E extends Exception>
    {
        T get() throws E;
    }
}
