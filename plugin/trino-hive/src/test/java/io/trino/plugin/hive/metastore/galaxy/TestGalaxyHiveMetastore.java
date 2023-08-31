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
package io.trino.plugin.hive.metastore.galaxy;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.AbstractTestHiveLocal;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.server.galaxy.GalaxyCockroachContainer;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;
import io.trino.testing.TestingConnectorSession;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.io.File;
import java.io.IOException;

import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.getHiveSessionProperties;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestGalaxyHiveMetastore
        extends AbstractTestHiveLocal
{
    private GalaxyCockroachContainer cockroach;
    private TestingGalaxyMetastore testingGalaxyMetastore;

    @BeforeClass(alwaysRun = true)
    @Override
    public void initialize()
            throws Exception
    {
        cockroach = new GalaxyCockroachContainer();
        testingGalaxyMetastore = new TestingGalaxyMetastore(cockroach);
        super.initialize();
    }

    @AfterClass(alwaysRun = true)
    @Override
    public void cleanup()
            throws IOException
    {
        super.cleanup();
        if (testingGalaxyMetastore != null) {
            testingGalaxyMetastore.close();
            testingGalaxyMetastore = null;
        }
        if (cockroach != null) {
            cockroach.close();
            cockroach = null;
        }
    }

    @Override
    protected ConnectorSession newSession()
    {
        return TestingConnectorSession.builder()
                .setPropertyMetadata(
                        ImmutableList.<PropertyMetadata<?>>builder()
                                .addAll(getHiveSessionProperties(getHiveConfig()).getSessionProperties())
                                .addAll(new GalaxyMetastoreSessionProperties().getSessionProperties())
                                .build())
                .build();
    }

    @Override
    protected HiveMetastore createMetastore(File tempDir)
    {
        return new GalaxyHiveMetastore(testingGalaxyMetastore.getMetastore(), HDFS_ENVIRONMENT, tempDir.getAbsolutePath(), new GalaxyHiveMetastoreConfig().isBatchMetadataFetch());
    }

    @Override
    public void testHideDeltaLakeTables()
    {
        assertThatThrownBy(super::testHideDeltaLakeTables)
                .hasMessageMatching("(?s)\n" +
                        "Expecting\n" +
                        "  \\[.*\\b(\\w+.tmp_trino_test_trino_delta_lake_table_\\w+)\\b.*]\n" +
                        "not to contain\n" +
                        "  \\[\\1]\n" +
                        "but found.*");

        throw new SkipException("not supported");
    }
}
