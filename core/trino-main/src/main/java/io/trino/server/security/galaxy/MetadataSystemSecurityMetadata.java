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
package io.trino.server.security.galaxy;

import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.metadata.SystemSecurityMetadata;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.security.GrantInfo;
import io.trino.spi.security.Identity;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.transaction.TransactionId;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

// NOTE: default handling of roles, etc. copied from DisabledSystemSecurityMetadata
public class MetadataSystemSecurityMetadata
        implements SystemSecurityMetadata
{
    private final Map<TransactionId, GalaxySecurityMetadata> securityMap = new ConcurrentHashMap<>();

    public void add(TransactionId transactionId, GalaxySecurityMetadata metadata)
    {
        securityMap.put(transactionId, metadata);
    }

    public void remove(TransactionId transactionId)
    {
        securityMap.remove(transactionId);
    }

    @Override
    public boolean roleExists(Session session, String role)
    {
        return false;
    }

    @Override
    public void createRole(Session session, String role, Optional<TrinoPrincipal> grantor)
    {
        throw new TrinoException(NOT_SUPPORTED, "Metadata roles are not enabled");
    }

    @Override
    public void dropRole(Session session, String role)
    {
        throw new TrinoException(NOT_SUPPORTED, "Metadata roles are not enabled");
    }

    @Override
    public Set<String> listRoles(Session session)
    {
        return ImmutableSet.of();
    }

    @Override
    public Set<RoleGrant> listRoleGrants(Session session, TrinoPrincipal principal)
    {
        return ImmutableSet.of();
    }

    @Override
    public void grantRoles(Session session, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        throw new TrinoException(NOT_SUPPORTED, "Metadata roles are not enabled");
    }

    @Override
    public void revokeRoles(Session session, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        throw new TrinoException(NOT_SUPPORTED, "Metadata roles are not enabled");
    }

    @Override
    public Set<RoleGrant> listApplicableRoles(Session session, TrinoPrincipal principal)
    {
        return ImmutableSet.of();
    }

    @Override
    public Set<String> listEnabledRoles(Identity identity)
    {
        TransactionId transactionId = MetadataAccessControllerSupplier.extractTransactionId(identity).orElseThrow(() -> new IllegalStateException("Transaction ID is not present"));
        return get(transactionId).listEnabledRoles(identity);
    }

    @Override
    public void grantSchemaPrivileges(Session session, CatalogSchemaName schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        throw notSupportedException(schemaName.getCatalogName());
    }

    @Override
    public void denySchemaPrivileges(Session session, CatalogSchemaName schemaName, Set<Privilege> privileges, TrinoPrincipal grantee)
    {
        throw notSupportedException(schemaName.getCatalogName());
    }

    @Override
    public void revokeSchemaPrivileges(Session session, CatalogSchemaName schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        throw notSupportedException(schemaName.getCatalogName());
    }

    @Override
    public void grantTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        throw notSupportedException(tableName.getCatalogName());
    }

    @Override
    public void denyTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee)
    {
        throw notSupportedException(tableName.getCatalogName());
    }

    @Override
    public void revokeTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        throw notSupportedException(tableName.getCatalogName());
    }

    @Override
    public Set<GrantInfo> listTablePrivileges(Session session, QualifiedTablePrefix prefix)
    {
        return ImmutableSet.of();
    }

    @Override
    public Optional<TrinoPrincipal> getSchemaOwner(Session session, CatalogSchemaName schema)
    {
        return Optional.empty();
    }

    @Override
    public void setSchemaOwner(Session session, CatalogSchemaName schema, TrinoPrincipal principal)
    {
        throw notSupportedException(schema.getCatalogName());
    }

    @Override
    public void setTableOwner(Session session, CatalogSchemaTableName table, TrinoPrincipal principal)
    {
        throw notSupportedException(table.getCatalogName());
    }

    @Override
    public Optional<Identity> getViewRunAsIdentity(Session session, CatalogSchemaTableName viewName)
    {
        return Optional.empty();
    }

    @Override
    public void setViewOwner(Session session, CatalogSchemaTableName view, TrinoPrincipal principal)
    {
        throw notSupportedException(view.getCatalogName());
    }

    @Override
    public void schemaCreated(Session session, CatalogSchemaName schema)
    {
        get(session).schemaCreated(session, schema);
    }

    @Override
    public void schemaRenamed(Session session, CatalogSchemaName sourceSchema, CatalogSchemaName targetSchema)
    {
        get(session).schemaRenamed(session, sourceSchema, targetSchema);
    }

    @Override
    public void schemaDropped(Session session, CatalogSchemaName schema)
    {
        get(session).schemaDropped(session, schema);
    }

    @Override
    public void tableCreated(Session session, CatalogSchemaTableName table)
    {
        get(session).tableCreated(session, table);
    }

    @Override
    public void tableRenamed(Session session, CatalogSchemaTableName sourceTable, CatalogSchemaTableName targetTable)
    {
        get(session).tableRenamed(session, sourceTable, targetTable);
    }

    @Override
    public void tableDropped(Session session, CatalogSchemaTableName table)
    {
        get(session).tableDropped(session, table);
    }

    @Override
    public void columnCreated(Session session, CatalogSchemaTableName table, String column)
    {
        get(session).columnCreated(session, table, column);
    }

    @Override
    public void columnRenamed(Session session, CatalogSchemaTableName table, String oldName, String newName)
    {
        get(session).columnRenamed(session, table, oldName, newName);
    }

    @Override
    public void columnDropped(Session session, CatalogSchemaTableName table, String column)
    {
        get(session).columnDropped(session, table, column);
    }

    private GalaxySecurityMetadata get(Session session)
    {
        TransactionId transactionId = session.getTransactionId().orElseThrow(() -> new IllegalStateException("Transaction ID is not present"));
        return get(transactionId);
    }

    private GalaxySecurityMetadata get(TransactionId transactionId)
    {
        GalaxySecurityMetadata securityMetadata = securityMap.get(transactionId);
        checkState(securityMetadata != null, "securityMetadata not set - unknown transaction: " + transactionId);
        return securityMetadata;
    }

    private static TrinoException notSupportedException(String catalogName)
    {
        return new TrinoException(NOT_SUPPORTED, "Catalog does not support permission management: " + catalogName);
    }
}
