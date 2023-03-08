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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.starburst.stargate.id.EntityKind;
import io.trino.spi.security.Privilege;
import io.trino.sql.tree.Node;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.StandardErrorCode.INVALID_PRIVILEGE;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;

public class PrivilegeUtilities
{
    private PrivilegeUtilities() {}

    private static final Map<EntityKind, Set<Privilege>> ENTITY_KIND_PRIVILEGES = ImmutableMap.of(
                    EntityKind.SCHEMA, ImmutableSet.of(Privilege.CREATE),
                    EntityKind.TABLE, ImmutableSet.of(Privilege.SELECT, Privilege.UPDATE, Privilege.INSERT, Privilege.DELETE));

    public static Set<Privilege> getPrivilegesForEntityKind(EntityKind entityKind)
    {
        Set<Privilege> privileges = ENTITY_KIND_PRIVILEGES.get(entityKind);
        if (privileges != null) {
            return privileges;
        }
        throw new IllegalArgumentException("Could not find privileges for EntityKind." + entityKind);
    }

    public static Set<Privilege> parseStatementPrivileges(Node statement, Optional<List<String>> optionalPrivileges, EntityKind entityKind)
    {
        Set<Privilege> privileges;
        if (optionalPrivileges.isPresent()) {
            privileges = optionalPrivileges.get().stream()
                    .map(privilege -> parsePrivilege(statement, privilege, entityKind))
                    .collect(toImmutableSet());
        }
        else {
            // All privileges
            privileges = getPrivilegesForEntityKind(entityKind);
        }
        return privileges;
    }

    private static Privilege parsePrivilege(Node statement, String privilegeString, EntityKind entityKind)
    {
        for (Privilege privilege : getPrivilegesForEntityKind(entityKind)) {
            if (privilege.name().equalsIgnoreCase(privilegeString)) {
                return privilege;
            }
        }

        throw semanticException(INVALID_PRIVILEGE, statement, "Unknown privilege: '%s'", privilegeString);
    }
}
