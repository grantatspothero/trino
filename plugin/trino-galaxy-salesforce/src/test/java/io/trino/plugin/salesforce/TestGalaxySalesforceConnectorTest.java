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
package io.trino.plugin.salesforce;

import com.starburstdata.trino.plugins.salesforce.TestSalesforceConnectorTest;

public class TestGalaxySalesforceConnectorTest
        extends TestSalesforceConnectorTest
{
    // Salesforce sandbox is shared across multiple repositories'(starburst-trino-plugins, galaxy-trino, stargate) CI jobs.
    // Tables names are suffixed with repository identifier to uniquely identify tables from respective repository.
    @Override
    protected String getRepositorySpecificTableName(String tableName)
    {
        return tableName + "_trino";
    }
}
