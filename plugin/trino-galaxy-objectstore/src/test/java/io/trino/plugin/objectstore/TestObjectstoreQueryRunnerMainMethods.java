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
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.util.AutoCloseableCloser;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.objectstore.ObjectStoreQueryRunner.run;
import static org.assertj.core.api.Assertions.assertThat;

public class TestObjectstoreQueryRunnerMainMethods
{
    @Test
    public void testCanInit()
            throws Exception
    {
        for (TableType tableType : TableType.values()) {
            try (AutoCloseableCloser closer = AutoCloseableCloser.create();
                    QueryRunner queryRunner = run(tableType, closer)) {
                assertThat(queryRunner.execute("SHOW CATALOGS")).contains(new MaterializedRow(ImmutableList.of("objectstore")));
            }
        }
    }
}