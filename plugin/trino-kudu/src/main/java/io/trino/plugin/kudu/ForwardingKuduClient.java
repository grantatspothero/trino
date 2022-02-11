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
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public abstract class ForwardingKuduClient
        implements KuduClientWrapper
{
    protected abstract <R> R delegate(Function<KuduClient, R> function);

    @Override
    public KuduTable createTable(String name, Schema schema, CreateTableOptions builder)
            throws KuduException
    {
        return delegateAndWrapKuduException((KuduClient kuduClient) -> kuduClient.createTable(name, schema, builder));
    }

    @Override
    public DeleteTableResponse deleteTable(String name)
            throws KuduException
    {
        return delegateAndWrapKuduException((KuduClient kuduClient) -> kuduClient.deleteTable(name));
    }

    @Override
    public AlterTableResponse alterTable(String name, AlterTableOptions ato)
    {
        return delegateAndWrapKuduException((KuduClient kuduClient) -> kuduClient.alterTable(name, ato));
    }

    @Override
    public ListTablesResponse getTablesList()
    {
        return delegateAndWrapKuduException((KuduClient kuduClient) -> kuduClient.getTablesList());
    }

    @Override
    public ListTablesResponse getTablesList(String nameFilter)
    {
        return delegateAndWrapKuduException((KuduClient kuduClient) -> kuduClient.getTablesList(nameFilter));
    }

    @Override
    public boolean tableExists(String name)
    {
        return delegateAndWrapKuduException((KuduClient kuduClient) -> kuduClient.tableExists(name));
    }

    @Override
    public KuduTable openTable(final String name)
    {
        return delegateAndWrapKuduException((KuduClient kuduClient) -> kuduClient.openTable(name));
    }

    @Override
    public KuduScanner.KuduScannerBuilder newScannerBuilder(KuduTable table)
    {
        return delegateAndWrapKuduException((KuduClient kuduClient) -> kuduClient.newScannerBuilder(table));
    }

    @Override
    public KuduScanToken.KuduScanTokenBuilder newScanTokenBuilder(KuduTable table)
    {
        return delegateAndWrapKuduException((KuduClient kuduClient) -> kuduClient.newScanTokenBuilder(table));
    }

    @Override
    public KuduSession newSession()
    {
        return delegateAndWrapKuduException((KuduClient kuduClient) -> kuduClient.newSession());
    }

    @Override
    public KuduScanner deserializeIntoScanner(byte[] serializedScanToken)
    {
        return delegateAndWrapIOException((KuduClient kuduClient) -> KuduScanToken.deserializeIntoScanner(serializedScanToken, kuduClient));
    }

    @Override
    public void close()
    {
        delegateAndWrapKuduException((KuduClient kuduClient) -> {
            kuduClient.close();
            return null;
        });
    }

    public static class UncheckedKuduException
            extends RuntimeException
    {
        UncheckedKuduException(KuduException cause)
        {
            super(requireNonNull(cause))
        }

        @Override
        public KuduException getCause() {
            return (KuduException) super.getCause();
        }
    }

    @FunctionalInterface
    protected interface CheckedFunction<T, R, E extends Exception>
    {
        R apply(T argument) throws E;
    }

    private <R> R delegateAndWrapKuduException(CheckedFunction<KuduClient, R, KuduException> checkedFunction)
            throws KuduException
    {
        Function<KuduClient, R> uncheckedFunction = (KuduClient kuduClient) -> {
            try {
                return checkedFunction.apply(kuduClient);
            }
            catch (KuduException e) {
                throw new UncheckedKuduException(e);
            }
        };
        try{
            return delegate(uncheckedFunction);
        }catch (UncheckedKuduException e){
            throw e.getCause();
        }
    }

    private <R> R delegateAndWrapIOException(CheckedFunction<KuduClient, R, IOException> checkedFunction)
            throws IOException
    {
        Function<KuduClient, R> uncheckedFunction = (KuduClient kuduClient) -> {
            try {
                return checkedFunction.apply(kuduClient);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
        try{
            return delegate(uncheckedFunction);
        }
        catch (UncheckedIOException e){
            throw e.getCause();
        }
    }
}
