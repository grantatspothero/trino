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
package io.trino.plugin.kudu.schema;

import io.trino.plugin.kudu.IKuduClient;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;

public interface SchemaEmulation
{
    void createSchema(IKuduClient client, String schemaName);

    void dropSchema(IKuduClient client, String schemaName);

    boolean existsSchema(IKuduClient client, String schemaName);

    List<String> listSchemaNames(IKuduClient client);

    String toRawName(SchemaTableName schemaTableName);

    SchemaTableName fromRawName(String rawName);

    String getPrefixForTablesOfSchema(String schemaName);

    List<String> filterTablesForDefaultSchema(List<String> rawTables);
}
