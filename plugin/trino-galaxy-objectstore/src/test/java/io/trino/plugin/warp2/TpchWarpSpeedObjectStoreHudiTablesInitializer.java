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
package io.trino.plugin.warp2;

import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.galaxy.GalaxyHiveMetastore;
import io.trino.plugin.hudi.testing.TpchHudiTablesInitializer;
import io.trino.testing.DistributedQueryRunner;
import io.trino.tpch.TpchTable;
import org.apache.hudi.common.model.HoodieTableType;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class TpchWarpSpeedObjectStoreHudiTablesInitializer
        extends TpchHudiTablesInitializer
{
    private final GalaxyHiveMetastore hiveMetastore;
    private final TrinoFileSystem trinoFileSystem;

    public TpchWarpSpeedObjectStoreHudiTablesInitializer(GalaxyHiveMetastore hiveMetastore, TrinoFileSystem trinoFileSystem, HoodieTableType hoodieTableType, List<TpchTable<?>> requiredTpchTables)
    {
        super(hoodieTableType, requiredTpchTables);
        this.hiveMetastore = requireNonNull(hiveMetastore, "hiveMetastore is null");
        this.trinoFileSystem = requireNonNull(trinoFileSystem, "trinoFileSystem is null");
    }

    @Override
    protected HiveMetastore getMetastore(DistributedQueryRunner queryRunner)
    {
        return hiveMetastore;
    }

    @Override
    protected TrinoFileSystem getTrinoFileSystem(DistributedQueryRunner queryRunner)
    {
        return trinoFileSystem;
    }
}
