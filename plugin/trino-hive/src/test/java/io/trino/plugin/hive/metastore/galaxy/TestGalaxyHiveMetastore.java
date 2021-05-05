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

import io.trino.plugin.hive.AbstractTestHiveLocal;
import io.trino.plugin.hive.metastore.HiveMetastore;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;

import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;

public class TestGalaxyHiveMetastore
        extends AbstractTestHiveLocal
{
    private TestingGalaxyMetastore testingGalaxyMetastore;

    @BeforeClass(alwaysRun = true)
    @Override
    public void initialize()
            throws Exception
    {
        testingGalaxyMetastore = new TestingGalaxyMetastore();
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
        }
        testingGalaxyMetastore = null;
    }

    @Override
    protected HiveMetastore createMetastore(File tempDir)
    {
        return new GalaxyHiveMetastore(testingGalaxyMetastore.getMetastore(), HDFS_ENVIRONMENT, tempDir.getAbsolutePath());
    }

    @Test(enabled = false)
    @Override
    public void testHideDeltaLakeTables()
    {
        // Delta tables aren't supported at all so there is no need to ignore them
    }
}
