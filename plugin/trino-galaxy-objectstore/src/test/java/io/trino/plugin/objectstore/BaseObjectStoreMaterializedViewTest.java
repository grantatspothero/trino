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

import io.trino.plugin.iceberg.BaseIcebergMaterializedViewTest;
import io.trino.testing.QueryFailedException;
import org.junit.jupiter.api.Test;

import static io.trino.server.security.galaxy.GalaxyTestHelper.ACCOUNT_ADMIN;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class BaseObjectStoreMaterializedViewTest
        extends BaseIcebergMaterializedViewTest
{
    @Test
    @Override
    public void testMaterializedViewOnTableRolledBack()
    {
        String schema = getSession().getSchema().orElseThrow();

        // There is no implicit grant for $snapshots table to the creator of mv_on_rolled_back_base_table table.
        assertThatThrownBy(super::testMaterializedViewOnTableRolledBack)
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("Access Denied: Cannot select from columns [snapshot_id, committed_at] in table or view " +
                        "iceberg." + schema + ".mv_on_rolled_back_base_table$snapshots: Relation not found or not allowed");
        // Cleanup after test flow interrupted
        assertUpdate("DROP MATERIALIZED VIEW mv_on_rolled_back_the_mv");
        assertUpdate("DROP TABLE mv_on_rolled_back_base_table");

        executeExclusively(() -> {
            // Access $snapshots table
            assertQuerySucceeds("GRANT SELECT ON \"*\" TO ROLE " + ACCOUNT_ADMIN);
            try {
                super.testMaterializedViewOnTableRolledBack();
            }
            finally {
                assertQuerySucceeds("REVOKE SELECT ON \"*\" FROM ROLE " + ACCOUNT_ADMIN);
            }
        });

        // Retest the initial state to ensure the state reset was full
        assertThatThrownBy(super::testMaterializedViewOnTableRolledBack)
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("Access Denied: Cannot select from columns [snapshot_id, committed_at] in table or view " +
                        "iceberg." + schema + ".mv_on_rolled_back_base_table$snapshots: Relation not found or not allowed");
        // Cleanup after test flow interrupted
        assertUpdate("DROP MATERIALIZED VIEW mv_on_rolled_back_the_mv");
        assertUpdate("DROP TABLE mv_on_rolled_back_base_table");
    }
}
