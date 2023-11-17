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
package io.trino.plugin.stargate;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestStargateWithStarburstEnterpriseLatestConnectorSmokeTest
        extends BaseStargateWithStarburstEnterpriseConnectorSmokeTest
{
    public TestStargateWithStarburstEnterpriseLatestConnectorSmokeTest()
    {
        super("latest");
    }

    @Override // Column definitions contain NOT NULL constraint in newer Starburst Enterprise
    @Test
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .isEqualTo("""
                        CREATE TABLE p2p_remote.tiny.region (
                           regionkey bigint NOT NULL,
                           name varchar(25) NOT NULL,
                           comment varchar(152) NOT NULL
                        )""");
    }
}
