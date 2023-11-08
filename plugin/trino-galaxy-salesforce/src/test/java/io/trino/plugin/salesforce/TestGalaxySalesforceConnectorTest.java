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
import io.trino.testing.MaterializedResult;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestGalaxySalesforceConnectorTest
        extends TestSalesforceConnectorTest
{
    // Overridden as the order of retrieved column is different
    @Override
    @Test
    public void testDescribeTable()
    {
        MaterializedResult expectedColumns = MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("id", "varchar(18)", "", "Label Record ID corresponds to this field.")
                .row("ownerid", "varchar(18)", "", "Label Owner ID corresponds to this field.")
                .row("isdeleted", "boolean", "", "Label Deleted corresponds to this field.")
                .row("name", "varchar(80)", "", "Label Name corresponds to this field.")
                .row("createddate", "timestamp(0)", "", "Label Created Date corresponds to this field.")
                .row("createdbyid", "varchar(18)", "", "Label Created By ID corresponds to this field.")
                .row("lastmodifieddate", "timestamp(0)", "", "Label Last Modified Date corresponds to this field.")
                .row("lastmodifiedbyid", "varchar(18)", "", "Label Last Modified By ID corresponds to this field.")
                .row("systemmodstamp", "timestamp(0)", "", "Label System Modstamp corresponds to this field.")
                .row("lastactivitydate", "date", "", "Label Last Activity Date corresponds to this field.")
                .row("orderstatus__c", "varchar(1)", "", "Label orderstatus corresponds to this field.")
                .row("custkey__c", "double", "", "Label custkey corresponds to this field.")
                .row("totalprice__c", "double", "", "Label totalprice corresponds to this field.")
                .row("shippriority__c", "double", "", "Label shippriority corresponds to this field.")
                .row("clerk__c", "varchar(15)", "", "Label clerk corresponds to this field.")
                .row("orderpriority__c", "varchar(15)", "", "Label orderpriority corresponds to this field.")
                .row("comment__c", "varchar(79)", "", "Label comment corresponds to this field.")
                .row("orderkey__c", "double", "", "Label orderkey corresponds to this field.")
                .row("orderdate__c", "date", "", "Label orderdate corresponds to this field.")
                .build();
        MaterializedResult actualColumns = computeActual("DESCRIBE " + salesforceOrdersTableName);
        assertThat(actualColumns).containsExactlyElementsOf(expectedColumns);
    }

    // Overridden as the order of retrieved column is different
    @Override
    @Test
    public void testShowColumns()
    {
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM " + salesforceOrdersTableName);
        MaterializedResult expected = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("id", "varchar(18)", "", "Label Record ID corresponds to this field.")
                .row("ownerid", "varchar(18)", "", "Label Owner ID corresponds to this field.")
                .row("isdeleted", "boolean", "", "Label Deleted corresponds to this field.")
                .row("name", "varchar(80)", "", "Label Name corresponds to this field.")
                .row("createddate", "timestamp(0)", "", "Label Created Date corresponds to this field.")
                .row("createdbyid", "varchar(18)", "", "Label Created By ID corresponds to this field.")
                .row("lastmodifieddate", "timestamp(0)", "", "Label Last Modified Date corresponds to this field.")
                .row("lastmodifiedbyid", "varchar(18)", "", "Label Last Modified By ID corresponds to this field.")
                .row("systemmodstamp", "timestamp(0)", "", "Label System Modstamp corresponds to this field.")
                .row("lastactivitydate", "date", "", "Label Last Activity Date corresponds to this field.")
                .row("orderstatus__c", "varchar(1)", "", "Label orderstatus corresponds to this field.")
                .row("custkey__c", "double", "", "Label custkey corresponds to this field.")
                .row("totalprice__c", "double", "", "Label totalprice corresponds to this field.")
                .row("shippriority__c", "double", "", "Label shippriority corresponds to this field.")
                .row("clerk__c", "varchar(15)", "", "Label clerk corresponds to this field.")
                .row("orderpriority__c", "varchar(15)", "", "Label orderpriority corresponds to this field.")
                .row("comment__c", "varchar(79)", "", "Label comment corresponds to this field.")
                .row("orderkey__c", "double", "", "Label orderkey corresponds to this field.")
                .row("orderdate__c", "date", "", "Label orderdate corresponds to this field.")
                .build();

        // Until we migrate all connectors to parametrized varchar we check two options
        assertThat(actual).containsExactlyElementsOf(expected);
    }

    // Overridden as the order of retrieved column is different
    @Override
    @Test
    public void testShowCreateTable()
    {
        String catalog = getSession().getCatalog().orElseThrow();
        String schema = getSession().getSchema().orElseThrow();
        assertThat(computeScalar("SHOW CREATE TABLE " + salesforceOrdersTableName))
                // If the connector reports additional column properties, the expected value needs to be adjusted in the test subclass
                .isEqualTo(format("""
                                 CREATE TABLE %s.%s.%s (
                                    id varchar(18) NOT NULL COMMENT 'Label Record ID corresponds to this field.',
                                    ownerid varchar(18) NOT NULL COMMENT 'Label Owner ID corresponds to this field.',
                                    isdeleted boolean NOT NULL COMMENT 'Label Deleted corresponds to this field.',
                                    name varchar(80) COMMENT 'Label Name corresponds to this field.',
                                    createddate timestamp(0) NOT NULL COMMENT 'Label Created Date corresponds to this field.',
                                    createdbyid varchar(18) NOT NULL COMMENT 'Label Created By ID corresponds to this field.',
                                    lastmodifieddate timestamp(0) COMMENT 'Label Last Modified Date corresponds to this field.',
                                    lastmodifiedbyid varchar(18) COMMENT 'Label Last Modified By ID corresponds to this field.',
                                    systemmodstamp timestamp(0) NOT NULL COMMENT 'Label System Modstamp corresponds to this field.',
                                    lastactivitydate date COMMENT 'Label Last Activity Date corresponds to this field.',
                                    orderstatus__c varchar(1) COMMENT 'Label orderstatus corresponds to this field.',
                                    custkey__c double COMMENT 'Label custkey corresponds to this field.',
                                    totalprice__c double COMMENT 'Label totalprice corresponds to this field.',
                                    shippriority__c double COMMENT 'Label shippriority corresponds to this field.',
                                    clerk__c varchar(15) COMMENT 'Label clerk corresponds to this field.',
                                    orderpriority__c varchar(15) COMMENT 'Label orderpriority corresponds to this field.',
                                    comment__c varchar(79) COMMENT 'Label comment corresponds to this field.',
                                    orderkey__c double COMMENT 'Label orderkey corresponds to this field.',
                                    orderdate__c date COMMENT 'Label orderdate corresponds to this field.'
                                 )""",
                        catalog, schema, salesforceOrdersTableName));
    }

    // Salesforce sandbox is shared across multiple repositories'(starburst-trino-plugins, galaxy-trino, stargate) CI jobs.
    // Tables names are suffixed with repository identifier to uniquely identify tables from respective repository.
    @Override
    protected String getRepositorySpecificTableName(String tableName)
    {
        return tableName + "_trino";
    }
}
