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
package io.trino.plugin.hive.metastore.thrift;

import com.google.common.collect.Lists;
import io.airlift.testing.Closeables;
import io.airlift.units.Duration;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.hive.thrift.metastore.NoSuchObjectException;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.sshtunnel.SshTunnelConfig;
import jakarta.servlet.http.HttpServletRequest;
import org.apache.http.HttpHeaders;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestThriftHttpMetastoreClient
{
    private static TestingThriftHttpMetastoreServer metastoreServer;
    private static TestRequestHeaderInterceptor requestHeaderInterceptor;
    private static final String httpToken = "test-token";
    private final HiveMetastoreAuthentication noAuthentication = new NoHiveMetastoreAuthentication();
    private final Duration timeout = new Duration(20, SECONDS);
    private static FileHiveMetastore delegate;

    private static final String testDbName = "testdb";

    @BeforeClass(alwaysRun = true)
    public static void setup()
            throws Exception
    {
        requestHeaderInterceptor = new TestRequestHeaderInterceptor();
        File tempDir = Files.createTempDirectory(null).toFile();
        tempDir.deleteOnExit();

        LocalFileSystemFactory fileSystemFactory = new LocalFileSystemFactory(tempDir.toPath());

        delegate = new FileHiveMetastore(
                new NodeVersion("testversion"),
                fileSystemFactory,
                false,
                new FileHiveMetastoreConfig()
                        .setCatalogDirectory("local:///")
                        .setMetastoreUser("test")
                        .setDisableLocationChecks(true));

        metastoreServer = new TestingThriftHttpMetastoreServer(delegate, requestHeaderInterceptor);
    }

    @AfterClass(alwaysRun = true)
    public static void tearDown()
            throws Exception
    {
        Closeables.closeAll(metastoreServer);
        metastoreServer = null;
    }

    @Test
    public void testHttpThriftConnection()
            throws Exception
    {
        Database.Builder database = Database.builder()
                .setDatabaseName(testDbName)
                .setOwnerName(Optional.empty())
                .setOwnerType(Optional.empty());
        Database db = database.build();
        delegate.createDatabase(db);

        ThriftMetastoreClientFactory factory = new DefaultThriftMetastoreClientFactory(
                new SshTunnelConfig(),
                Optional.empty(),
                Optional.empty(),
                timeout,
                timeout,
                noAuthentication,
                "localhost",
                DefaultThriftMetastoreClientFactory.buildThriftHttpContext(getHttpMetastoreConfig()),
                OpenTelemetry.noop());
        URI metastoreUri = URI.create("http://localhost:" + metastoreServer.getPort());
        ThriftMetastoreClient client = factory.create(
                metastoreUri, Optional.empty());
        assertThat(client.getAllDatabases()).isEqualTo(Lists.newArrayList(testDbName));
        assertThat(requestHeaderInterceptor.getInterceptedHeader("key1")).isEqualTo("value1");
        assertThat(requestHeaderInterceptor.getInterceptedHeader("key2")).isEqualTo("value2");
        assertThat(requestHeaderInterceptor.getInterceptedHeader(HttpHeaders.AUTHORIZATION)).isEqualTo("Bearer " + httpToken);
        // negative case
        assertThatThrownBy(() -> client.getDatabase("does-not-exist"))
                .isInstanceOf(NoSuchObjectException.class);
    }

    private static ThriftHttpMetastoreConfig getHttpMetastoreConfig()
    {
        ThriftHttpMetastoreConfig config = new ThriftHttpMetastoreConfig();
        config.setHttpBearerToken(httpToken);
        config.setAdditionalHeaders(List.of("key1:value1", "key2:value2"));
        return config;
    }

    private static class TestRequestHeaderInterceptor
            implements Consumer<HttpServletRequest>
    {
        private final Map<String, String> requestHeaders = new HashMap<>();

        public String getInterceptedHeader(String headerName)
        {
            return requestHeaders.get(headerName);
        }

        @Override
        public void accept(HttpServletRequest httpServletRequest)
        {
            requestHeaders.clear();
            requestHeaders.put("key1", httpServletRequest.getHeader("key1"));
            requestHeaders.put("key2", httpServletRequest.getHeader("key2"));
            requestHeaders.put(HttpHeaders.AUTHORIZATION, httpServletRequest.getHeader(HttpHeaders.AUTHORIZATION));
        }
    }
}
