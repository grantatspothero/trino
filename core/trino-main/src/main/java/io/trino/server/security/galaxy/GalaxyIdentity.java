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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.starburst.stargate.id.AccountId;
import io.starburst.stargate.id.RoleId;
import io.starburst.stargate.id.RoleName;
import io.starburst.stargate.id.UserId;
import io.starburst.stargate.identity.DispatchSession;
import io.trino.Session;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SystemSecurityContext;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class GalaxyIdentity
{
    private static final String GALAXY_TOKEN_CREDENTIAL = "GalaxyTokenCredential";
    /**
     * The user name provided for a view must include the RoleId of the
     * owner of the view, because Trino in effect indexes view column
     * references by the user name.
     */
    private static final String GALAXY_VIEW_OWNER_USER_NAME_TEMPLATE = "<galaxy role %s>";
    private static final Splitter PRINCIPAL_SPLITTER = Splitter.on(':').limit(5);

    private GalaxyIdentity() {}

    public enum GalaxyIdentityType
    {
        DEFAULT,
        INDEXING,
    }

    public static GalaxyIdentityType toGalaxyIdentityType(String value)
    {
        return "galaxy_indexer".equals(value) ? GalaxyIdentityType.INDEXING : GalaxyIdentityType.DEFAULT;
    }

    public static GalaxyIdentityType getGalaxyIdentityType(Identity identity)
    {
        String identityType = identity.getExtraCredentials().get("identityType");
        return GalaxyIdentityType.INDEXING.name().equals(identityType) ? GalaxyIdentityType.INDEXING : GalaxyIdentityType.DEFAULT;
    }

    public static Identity createIdentity(String username, AccountId accountId, UserId userId, RoleId roleId, String token, GalaxyIdentityType identityType)
    {
        return Identity.forUser(username)
                .withPrincipal(createPrincipal(accountId, userId, roleId))
                .withExtraCredentials(Map.of(
                        "accountId", accountId.toString(),
                        "userId", userId.toString(),
                        "roleId", roleId.toString(),
                        "identityType", identityType.name(),
                        GALAXY_TOKEN_CREDENTIAL, token))
                .build();
    }

    public static Identity createIdentity(String username, AccountId accountId, UserId userId, RoleId roleId, Set<String> enabledRoles, String token, GalaxyIdentityType identityType)
    {
        return Identity.forUser(username)
                .withPrincipal(createPrincipal(accountId, userId, roleId))
                .withExtraCredentials(Map.of(
                        "accountId", accountId.toString(),
                        "userId", userId.toString(),
                        "roleId", roleId.toString(),
                        "identityType", identityType.name(),
                        GALAXY_TOKEN_CREDENTIAL, token))
                .withEnabledRoles(enabledRoles)
                .build();
    }

    private static BasicPrincipal createPrincipal(AccountId accountId, UserId userId, RoleId roleId)
    {
        return new BasicPrincipal(createPrincipalString(accountId, userId, roleId));
    }

    @VisibleForTesting
    static String createPrincipalString(AccountId accountId, UserId userId, RoleId roleId)
    {
        return format("galaxy:%s:%s:%s", accountId, userId, roleId);
    }

    public static RoleId getRoleId(Identity identity)
    {
        return new RoleId(splitPrincipal(identity).get(3));
    }

    public static DispatchSession toDispatchSession(Session session)
    {
        return toDispatchSession(session.getIdentity());
    }

    public static RoleId getContextRoleId(SystemSecurityContext context)
    {
        String roleIdString = context.getIdentity().getExtraCredentials().get("roleId");
        if (roleIdString == null) {
            roleIdString = splitPrincipal(context.getIdentity()).get(3);
        }
        return new RoleId(roleIdString);
    }

    public static DispatchSession toDispatchSession(Identity identity)
    {
        List<String> parts = splitPrincipal(identity);
        return new DispatchSession(
                new AccountId(parts.get(1)),
                new UserId(parts.get(2)),
                new RoleId(parts.get(3)),
                getGalaxyToken(identity));
    }

    public static Identity createViewOwnerIdentity(Identity identity, RoleName viewOwnerRoleName, RoleId viewOwnerId)
    {
        // Return an Identity with the synthetic user, the updated credentials, and enabled
        // roles consisting only of the viewOwnerId
        return Identity.from(identity)
                .withUser(GALAXY_VIEW_OWNER_USER_NAME_TEMPLATE.formatted(viewOwnerId))
                .withEnabledRoles(ImmutableSet.of(viewOwnerRoleName.getName()))
                .withAdditionalExtraCredentials(ImmutableMap.<String, String>builder()
                        .put("roleId", viewOwnerId.toString())
                        .buildOrThrow())
                .build();
    }

    private static String getGalaxyToken(Identity identity)
    {
        return requireNonNull(identity.getExtraCredentials().get(GALAXY_TOKEN_CREDENTIAL), "token is null");
    }

    private static List<String> splitPrincipal(Identity identity)
    {
        String principal = identity.getPrincipal().orElseThrow(() -> new IllegalArgumentException("Identity does not contain a principal: " + identity)).toString();
        List<String> parts = PRINCIPAL_SPLITTER.splitToList(principal);
        if (parts.size() != 4 || !parts.get(0).equals("galaxy")) {
            throw new IllegalArgumentException("Invalid Galaxy principal: " + principal);
        }
        return parts;
    }
}
