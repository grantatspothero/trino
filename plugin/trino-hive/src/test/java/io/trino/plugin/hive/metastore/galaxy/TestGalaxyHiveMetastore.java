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

import io.trino.plugin.hive.metastore.AbstractTestHiveMetastore;
import io.trino.server.galaxy.GalaxyCockroachContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;
import java.nio.file.Path;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static java.nio.file.Files.createTempDirectory;

public class TestGalaxyHiveMetastore
        extends AbstractTestHiveMetastore
{
    private GalaxyCockroachContainer cockroach;
    private TestingGalaxyMetastore testingGalaxyMetastore;
    private Path tempDir;

    @BeforeAll
    public void initialize()
            throws Exception
    {
        tempDir = createTempDirectory("test");
        tempDir.toFile().mkdirs();
        cockroach = new GalaxyCockroachContainer();
        testingGalaxyMetastore = new TestingGalaxyMetastore(cockroach);
        setMetastore(new GalaxyHiveMetastore(testingGalaxyMetastore.getMetastore(), HDFS_FILE_SYSTEM_FACTORY, tempDir.toUri().toString(), new GalaxyHiveMetastoreConfig().isBatchMetadataFetch()));
    }

    @AfterAll
    public void cleanup()
            throws IOException
    {
        if (testingGalaxyMetastore != null) {
            testingGalaxyMetastore.close();
            testingGalaxyMetastore = null;
        }
        if (cockroach != null) {
            cockroach.close();
            cockroach = null;
        }
        deleteRecursively(tempDir, ALLOW_INSECURE);
    }
}
