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
package io.trino.plugin.cassandra.galaxy.astradb;

import com.datastax.oss.driver.api.core.CqlSession;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import io.trino.plugin.cassandra.CassandraSession;
import io.trino.plugin.cassandra.CassandraTypeManager;
import io.trino.plugin.cassandra.ExtraColumnMetadata;

import java.util.List;
import java.util.function.Supplier;

public class AstraDbSession
        extends CassandraSession
{
    public AstraDbSession(CassandraTypeManager cassandraTypeManager, JsonCodec<List<ExtraColumnMetadata>> extraColumnMetadataCodec, Supplier<CqlSession> sessionSupplier, Duration noHostAvailableRetryTimeout)
    {
        super(cassandraTypeManager, extraColumnMetadataCodec, sessionSupplier, noHostAvailableRetryTimeout);
    }

    @Override
    protected void checkSizeEstimatesTableExist()
    {
        // Could not find a way to get the `system` keyspace(and `size_estimates` table) through client, though directly querying `system.size_estimates` table works.
    }
}
