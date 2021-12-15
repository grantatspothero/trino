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
package io.trino.execution;

import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.sql.tree.Statement;

import static io.trino.spi.StandardErrorCode.CATALOG_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.SYNTAX_ERROR;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;

public final class GalaxyWildcardUtil
{
    private GalaxyWildcardUtil() {}

    public static boolean isWildcard(CatalogSchemaName schemaName)
    {
        return isWildcard(schemaName.getCatalogName()) || isWildcard(schemaName.getSchemaName());
    }

    public static boolean isWildcard(QualifiedObjectName tableName)
    {
        return isWildcard(tableName.getCatalogName()) || isWildcard(tableName.getSchemaName()) || isWildcard(tableName.getObjectName());
    }

    private static boolean isWildcard(String name)
    {
        return name.equals("*");
    }

    public static void validateWildcard(Session session, Metadata metadata, Statement statement, CatalogSchemaName schemaName)
    {
        if (isWildcard(schemaName.getCatalogName())) {
            throw semanticException(SYNTAX_ERROR, statement, "Catalog wildcard is not allowed: %s", schemaName);
        }
        if (!metadata.catalogExists(session, schemaName.getCatalogName())) {
            throw semanticException(CATALOG_NOT_FOUND, statement, "Catalog '%s' does not exist", schemaName.getCatalogName());
        }
    }

    public static void validateWildcard(Session session, Metadata metadata, Statement statement, QualifiedObjectName tableName)
    {
        if (isWildcard(tableName.getCatalogName())) {
            throw semanticException(SYNTAX_ERROR, statement, "Catalog wildcard is not allowed: %s", tableName);
        }

        if (isWildcard(tableName.getSchemaName())) {
            if (!isWildcard(tableName.asSchemaTableName().getTableName())) {
                throw semanticException(SYNTAX_ERROR, statement, "Schema wildcard requires a table wildcard: %s", tableName);
            }
            if (!metadata.catalogExists(session, tableName.getCatalogName())) {
                throw semanticException(CATALOG_NOT_FOUND, statement, "Catalog '%s' does not exist", tableName.getCatalogName());
            }
        }
        else if (isWildcard(tableName.getObjectName())) {
            CatalogSchemaName schemaName = new CatalogSchemaName(tableName.getCatalogName(), tableName.getSchemaName());
            if (!metadata.schemaExists(session, schemaName)) {
                throw semanticException(SCHEMA_NOT_FOUND, statement, "Schema '%s' does not exist", schemaName);
            }
        }
    }
}
