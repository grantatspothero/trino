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
package io.trino.plugin.objectstore;

import com.google.common.collect.ImmutableList;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.spi.connector.ConnectorFactory;

import static java.util.Objects.requireNonNull;

public class TestingObjectStorePlugin
        extends ObjectStorePlugin
{
    private final HiveMetastore metastore;
    private final TrinoFileSystemFactory fileSystemFactory;

    public TestingObjectStorePlugin(HiveMetastore metastore, TrinoFileSystemFactory fileSystemFactory)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new TestingObjectStoreConnectorFactory(metastore, fileSystemFactory));
    }
}