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
package io.trino.plugin.hudi.testing;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hudi.HudiConnector;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.TransactionBuilder;

import static io.trino.plugin.hudi.testing.TpchHudiTablesInitializer.FIELD_UUID;
import static org.apache.hudi.common.model.HoodieRecord.HOODIE_META_COLUMNS;

public final class HudiTestUtils
{
    private static final String HUDI_CATALOG = "hudi";

    private HudiTestUtils() {}

    public static final String COLUMNS_TO_HIDE = String.join(",", ImmutableList.<String>builder()
            .addAll(HOODIE_META_COLUMNS)
            .add(FIELD_UUID)
            .build());

    public static <T> T getConnectorService(LocalQueryRunner queryRunner, Class<T> clazz)
    {
        return ((HudiConnector) queryRunner.getConnector(HUDI_CATALOG)).getInjector().getInstance(clazz);
    }

    public static <T> T getConnectorService(DistributedQueryRunner queryRunner, Class<T> clazz)
    {
        return TransactionBuilder.transaction(queryRunner.getTransactionManager(), queryRunner.getPlannerContext().getMetadata(), queryRunner.getAccessControl())
                .readOnly()
                .execute(queryRunner.getDefaultSession(), transactionSession -> {
                    return ((HudiConnector) queryRunner.getCoordinator().getConnector(transactionSession, HUDI_CATALOG)).getInjector().getInstance(clazz);
                });
    }
}
