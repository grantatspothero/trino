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
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.ListTablesResponse;

public interface KuduClientWrapper
        extends AutoCloseable
{
    KuduTable createTable(String name, Schema schema, CreateTableOptions builder);

    DeleteTableResponse deleteTable(String name);

    AlterTableResponse alterTable(String name, AlterTableOptions ato);

    ListTablesResponse getTablesList();

    ListTablesResponse getTablesList(String nameFilter);

    boolean tableExists(String name);

    KuduTable openTable(String name);

    KuduScanner.KuduScannerBuilder newScannerBuilder(KuduTable table);

    KuduScanToken.KuduScanTokenBuilder newScanTokenBuilder(KuduTable table);

    KuduSession newSession();

    KuduScanner deserializeIntoScanner(byte[] serializedScanToken);

    @Override
    void close();
}
