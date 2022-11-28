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
package io.trino.plugin.bigquery;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQuery.DatasetDeleteOption;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.trino.plugin.base.galaxy.CrossRegionConfig;
import io.trino.plugin.base.galaxy.LocalRegionConfig;
import io.trino.plugin.base.logging.FormatInterpolator;
import io.trino.plugin.base.logging.SessionInterpolatedValues;
import io.trino.plugin.bigquery.IdentityCacheMapping.SingletonIdentityCacheMapping;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.galaxy.CatalogNetworkMonitor;
import io.trino.testing.TestingConnectorSession;
import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import java.util.Optional;
import java.util.Set;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(Lifecycle.PER_CLASS)
public class TestBigQueryNetworkUsageTracking
{
    private static final String BIGQUERY_CREDENTIALS_KEY = requireNonNull(System.getProperty("bigquery.credentials-key"), "bigquery.credentials-key is not set");
    private static final String CATALOG_NAME = "catalog";
    private static final String CATALOG_ID = "%s:normal:1".formatted(CATALOG_NAME);
    private static final DataSize CROSS_REGION_READ_LIMIT = DataSize.of(1, Unit.GIGABYTE);
    private static final DataSize CROSS_REGION_WRITE_LIMIT = DataSize.of(1, Unit.GIGABYTE);
    private static final long MAX_BATCH_SIZE = 50_000;

    private BigQuery bigQuery;
    private BigQueryReadClient bigQueryReadClient;

    @BeforeAll
    public void init()
    {
        BigQueryConfig bigQueryConfig = new BigQueryConfig();
        CatalogHandle catalogHandle = CatalogHandle.fromId(CATALOG_ID);
        LocalRegionConfig localRegionConfig = new LocalRegionConfig()
                // forces network traffic to be cross-region
                .setAllowedIpAddresses(ImmutableList.of("0.0.0.0"));
        CrossRegionConfig crossRegionConfig = new CrossRegionConfig()
                .setAllowCrossRegionAccess(true)
                .setCrossRegionReadLimit(CROSS_REGION_READ_LIMIT)
                .setCrossRegionWriteLimit(CROSS_REGION_WRITE_LIMIT);
        GalaxyTransportChannelProviderConfigurer galaxyTransportChannelProviderConfigurer =
                new GalaxyTransportChannelProviderConfigurer(catalogHandle, localRegionConfig, crossRegionConfig);
        CredentialsOptionsConfigurer credentialsOptionsConfigurer = new CredentialsOptionsConfigurer(
                bigQueryConfig,
                new StaticBigQueryCredentialsSupplier(
                        new StaticCredentialsConfig()
                                .setCredentialsKey(BIGQUERY_CREDENTIALS_KEY),
                        Optional.empty()));
        Set<BigQueryOptionsConfigurer> configurers = ImmutableSet.of(galaxyTransportChannelProviderConfigurer, credentialsOptionsConfigurer);
        ConnectorSession testingConnectorSession = TestingConnectorSession.builder()
                .setPropertyMetadata(new BigQuerySessionProperties(new BigQueryConfig()).getSessionProperties())
                .build();

        BigQueryClientFactory bigQueryClientFactory = createBigQueryClientFactory(bigQueryConfig, configurers);
        bigQuery = bigQueryClientFactory.createBigQuery(testingConnectorSession);

        BigQueryReadClientFactory bigQueryReadClientFactory = createBigQueryReadClientFactory(configurers);
        bigQueryReadClient = bigQueryReadClientFactory.create(testingConnectorSession);
    }

    @AfterAll
    public void cleanup()
    {
        bigQuery = null;
        if (bigQueryReadClient != null) {
            bigQueryReadClient.close();
            bigQueryReadClient = null;
        }
    }

    @Test
    public void testBigQueryClientNetworkTracking()
    {
        CatalogNetworkMonitor catalogNetworkMonitor = CatalogNetworkMonitor.getCrossRegionCatalogNetworkMonitor(CATALOG_NAME, CATALOG_ID, CROSS_REGION_READ_LIMIT.toBytes(), CROSS_REGION_WRITE_LIMIT.toBytes());
        String schemaName = "network_usage_test_" + randomNameSuffix();
        String tableName = "temp_table_" + randomNameSuffix();
        bigQuery.create(DatasetInfo.newBuilder(schemaName).build());

        TableId tableId = TableId.of(schemaName, tableName);
        TableDefinition tableDefinition = StandardTableDefinition.of(Schema.of(
                Field.of("value1", StandardSQLTypeName.INT64),
                Field.of("value2", StandardSQLTypeName.INT64),
                Field.of("value3", StandardSQLTypeName.INT64),
                Field.of("value4", StandardSQLTypeName.INT64)));
        bigQuery.create(TableInfo.of(tableId, tableDefinition));

        InsertAllRequest.Builder batch = InsertAllRequest.newBuilder(tableId);
        for (int i = 0; i < MAX_BATCH_SIZE; i += 4) {
            batch.addRow(ImmutableMap.of("value1", i, "value2", i + 1, "value3", i + 2, "value4", i + 3));
        }
        try {
            long previousWriteBytes = catalogNetworkMonitor.getCrossRegionWriteBytes();
            bigQuery.insertAll(batch.build());

            // the number of bytes tracked is fairly consistent across test runs
            long expectedWriteBytes = 134_398;
            assertThat(catalogNetworkMonitor.getCrossRegionWriteBytes() - previousWriteBytes).isCloseTo(expectedWriteBytes, Percentage.withPercentage(0.1));

            long previouslyReadBytes = catalogNetworkMonitor.getCrossRegionReadBytes();
            bigQuery.query(QueryJobConfiguration.newBuilder("SELECT * FROM %s.%s".formatted(schemaName, tableName)).build());

            // the difference here is a lot more variable than the written bytes, although still relatively consistent
            long expectedReadBytes = 139_953;
            assertThat(catalogNetworkMonitor.getCrossRegionReadBytes() - previouslyReadBytes).isCloseTo(expectedReadBytes, Percentage.withPercentage(0.5));

            // test network usage tracking for big query read client (rpc)
            String projectId = bigQuery.getOptions().getProjectId();
            CreateReadSessionRequest createReadSessionRequest = CreateReadSessionRequest.newBuilder()
                    .setParent("projects/%s".formatted(projectId))
                    .setReadSession(ReadSession.newBuilder()
                            .setDataFormat(DataFormat.AVRO)
                            .setTable("projects/%s/datasets/%s/tables/%s".formatted(projectId, schemaName, tableName)))
                    .setMaxStreamCount(1)
                    .build();
            ReadSession readSession = bigQueryReadClient.createReadSession(createReadSessionRequest);

            previouslyReadBytes = catalogNetworkMonitor.getCrossRegionReadBytes();
            ServerStream<ReadRowsResponse> responses = bigQueryReadClient.readRowsCallable()
                    .call(ReadRowsRequest.newBuilder()
                            .setReadStream(readSession.getStreams(0).getName())
                            .build());

            // fake ingest the rows so that they actually get transferred over the network
            long rowCount = 0;
            for (ReadRowsResponse response : responses) {
                rowCount += response.getRowCount();
            }
            assertThat(rowCount).isEqualTo(12500);

            // this expected value is pretty consistent
            expectedReadBytes = 195_851;
            // subtract previously read bytes as those have already been checked
            assertThat(catalogNetworkMonitor.getCrossRegionReadBytes() - previouslyReadBytes).isCloseTo(expectedReadBytes, Percentage.withPercentage(0.1));
        }
        catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
        finally {
            bigQuery.delete(tableId);
            bigQuery.delete(DatasetId.of(schemaName), DatasetDeleteOption.deleteContents());
        }
    }

    private BigQueryClientFactory createBigQueryClientFactory(
            BigQueryConfig bigQueryConfig,
            Set<BigQueryOptionsConfigurer> configurers)
    {
        return new BigQueryClientFactory(
                new SingletonIdentityCacheMapping(),
                new BigQueryTypeManager(TESTING_TYPE_MANAGER),
                bigQueryConfig,
                new ViewMaterializationCache(bigQueryConfig),
                new BigQueryLabelFactory(
                        bigQueryConfig.getQueryLabelName(),
                        new FormatInterpolator<>(bigQueryConfig.getQueryLabelFormat(), SessionInterpolatedValues.values())),
                configurers);
    }

    private BigQueryReadClientFactory createBigQueryReadClientFactory(
            Set<BigQueryOptionsConfigurer> configurers)
    {
        return new BigQueryReadClientFactory(configurers);
    }
}
