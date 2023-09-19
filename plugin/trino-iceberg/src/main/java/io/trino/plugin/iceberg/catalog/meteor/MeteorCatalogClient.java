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
package io.trino.plugin.iceberg.catalog.meteor;

import io.airlift.http.client.HttpClient;
import io.airlift.json.JsonCodec;
import io.trino.spi.connector.SchemaTableName;

import java.net.URI;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.prepareHead;
import static io.airlift.json.JsonCodec.listJsonCodec;

public class MeteorCatalogClient
        extends AbstractApiClient
{
    public static final JsonCodec<List<SchemaTableName>> SCHEMA_TABLE_NAME_LIST_JSON_CODEC = listJsonCodec(SchemaTableName.class);
    private static final String BASE_PATH = "/api/v1/meteor/query";

    public MeteorCatalogClient(HttpClient client, URI baseUrl)
    {
        super(client, baseUrl, BASE_PATH);
    }

    public boolean namespaceExists(String accountId, String catalogId, String namespace)
    {
        return executeHead(prepareHead()
                .setUri(uriFor(
                        Optional.empty(),
                        Optional.empty(),
                        "account", accountId,
                        "catalog", catalogId,
                        "schema", namespace))
                .build());
    }

    public List<String> listNamespaces(String accountId, String catalogId)
    {
        List<SchemaTableName> schemaTableNameList = listTables(accountId, catalogId, Optional.empty());

        return schemaTableNameList.stream()
                .map(SchemaTableName::getSchemaName)
                .distinct()
                .collect(toImmutableList());
    }

    public List<SchemaTableName> listTables(String accountId, String catalogId, Optional<String> namespace)
    {
        return execute(prepareGet()
                .setUri(uriFor(
                        Optional.empty(),
                        namespace.isPresent() ? Optional.of("schemaName=%s".formatted(namespace.get())) : Optional.empty(),
                        "account", accountId,
                        "catalog", catalogId))
                .build(), SCHEMA_TABLE_NAME_LIST_JSON_CODEC);
    }

    public String fetchMetadataLocation(String accountId, String catalogId, SchemaTableName schemaTableName)
    {
        return execute(prepareGet()
                .setUri(uriFor(
                        Optional.empty(),
                        Optional.empty(),
                        "account", accountId,
                        "catalog", catalogId,
                        "schema", schemaTableName.getSchemaName(),
                        "table", schemaTableName.getTableName(),
                        "metadata"))
                .build());
    }
}
