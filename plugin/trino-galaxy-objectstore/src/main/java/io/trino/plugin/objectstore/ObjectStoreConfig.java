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

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.configuration.Config;
import jakarta.validation.constraints.NotNull;

public class ObjectStoreConfig
{
    public enum InformationSchemaQueriesAcceleration {
        NONE,
        /**
         * Query underlying connectors in parallel
         */
        V1,
    }

    private TableType tableType = TableType.HIVE;
    private InformationSchemaQueriesAcceleration informationSchemaQueriesAcceleration = InformationSchemaQueriesAcceleration.NONE;

    @NotNull
    public TableType getTableType()
    {
        return tableType;
    }

    @CanIgnoreReturnValue
    @Config("object-store.table-type")
    public ObjectStoreConfig setTableType(TableType tableType)
    {
        this.tableType = tableType;
        return this;
    }

    public InformationSchemaQueriesAcceleration getInformationSchemaQueriesAcceleration()
    {
        return informationSchemaQueriesAcceleration;
    }

    @Config("object-store.information-schema-queries-acceleration")
    public ObjectStoreConfig setInformationSchemaQueriesAcceleration(InformationSchemaQueriesAcceleration informationSchemaQueriesAcceleration)
    {
        this.informationSchemaQueriesAcceleration = informationSchemaQueriesAcceleration;
        return this;
    }
}
