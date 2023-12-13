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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class GalaxyIdentity
{
    private static final String GALAXY_TOKEN_CREDENTIAL = "GalaxyTokenCredential";
    /**
     * For row filter and column mask identity, we encrypt the access key segment
     * of the user identity string when creating the ViewExpression, and decrypt
     * it before making a call to the portal-server using that identity.
     */
    private static final GalaxyIdentityCrypto crypto = new GalaxyIdentityCrypto();

    /**
     * The user name provided for a view must include the RoleId of the
     * owner of the view, because Trino in effect indexes view column
     * references by the user name.
     */
    private static final String GALAXY_VIEW_OWNER_USER_NAME_TEMPLATE = "<galaxy role %s>";
    /**
     * For row filters and column masks, the identity used for access control
     * is encoded in the user string of the Identity object.  It can be recognized
     * from its format, which is &lt;galaxy:accountId:userId:owningRoleId:encrypted_access_token&gt;
     */
    private static final Pattern GALAXY_USER_STRING_IDENTITY_MATCHER = Pattern.compile("<galaxy(:[^:]+){4}>");
    private static final Splitter PRINCIPAL_SPLITTER = Splitter.on(':').limit(5);
    public static final String PORTAL_IDENTITY_TYPE = "galaxy_portal";
    public static final String INDEXER_IDENTITY_TYPE = "galaxy_indexer";
    public static final String DISPATCH_IDENTITY_TYPE = "galaxy_dispatch";

    private GalaxyIdentity() {}

    public enum GalaxyIdentityType
    {
        PORTAL(PORTAL_IDENTITY_TYPE),
        INDEXER(INDEXER_IDENTITY_TYPE),
        DISPATCH(DISPATCH_IDENTITY_TYPE);

        private final String type;

        public String type()
        {
            return type;
        }

        GalaxyIdentityType(String type)
        {
            this.type = requireNonNull(type, "type is null");
        }
    }

    public static GalaxyIdentityType toGalaxyIdentityType(String value)
    {
        if (value == null) {
            return GalaxyIdentityType.PORTAL;
        }
        return switch (value) {
            case INDEXER_IDENTITY_TYPE -> GalaxyIdentityType.INDEXER;
            case DISPATCH_IDENTITY_TYPE -> GalaxyIdentityType.DISPATCH;
            default -> GalaxyIdentityType.PORTAL;
        };
    }

    public static GalaxyIdentityType getGalaxyIdentityType(Identity identity)
    {
        String identityType = identity.getExtraCredentials().get("identityType");
        try {
            return GalaxyIdentityType.valueOf(identityType);
        }
        catch (IllegalArgumentException e) {
            // ignore
        }
        return GalaxyIdentityType.PORTAL;
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

    /**
     * If the user in the identity object starts with "&lt;galaxy:", get
     * the roleId from the user string.  Otherwise, if the roleId is
     * in the extraCredentials, return that roleId.  If neither of
     * these works, get the roleId by splitting the principal string
     */
    public static RoleId getContextRoleId(Identity identity)
    {
        String roleIdString;
        if (isIdentityEncodedInTheUserString(identity)) {
            roleIdString = PRINCIPAL_SPLITTER.splitToList(identity.getUser()).get(3);
        }
        else {
            roleIdString = identity.getExtraCredentials().get("roleId");
            if (roleIdString == null) {
                roleIdString = splitPrincipal(identity).get(3);
            }
        }
        return new RoleId(roleIdString);
    }

    public static DispatchSession toDispatchSession(Identity identity)
    {
        if (isIdentityEncodedInTheUserString(identity)) {
            return toDispatchSessionFromUserString(identity.getUser());
        }
        return toDispatchSessionFromPrincipal(identity);
    }

    public static DispatchSession toDispatchSessionFromPrincipal(Identity identity)
    {
        List<String> parts = splitPrincipal(identity);
        return new DispatchSession(
                new AccountId(parts.get(1)),
                new UserId(parts.get(2)),
                new RoleId(parts.get(3)),
                getGalaxyToken(identity));
    }

    /**
     * Return a DispatchSession whose accountId, userId, roleId and token are
     * extracted from the user string.  This form identity is used in the
     * Optional&lt;String&gt; ViewExpression.identity for Galaxy row filters
     * and column masks.
     */
    private static DispatchSession toDispatchSessionFromUserString(String user)
    {
        List<String> parts = PRINCIPAL_SPLITTER.splitToList(user.substring(1, user.length() - 1));
        return new DispatchSession(
                new AccountId(parts.get(1)),
                new UserId(parts.get(2)),
                new RoleId(parts.get(3)),
                crypto.decryptToUtf8(parts.get(4)));
    }

    public static Identity createViewOrFunctionOwnerIdentity(Identity identity, RoleName viewOwnerRoleName, RoleId viewOwnerId)
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

    public static Optional<String> getRowFilterAndColumnMaskUserString(Identity identity, RoleId owningRoleId)
    {
        if (identity.getPrincipal().isPresent()) {
            String name = identity.getPrincipal().get().getName();
            if (name.startsWith("galaxy:")) {
                List<String> parts = PRINCIPAL_SPLITTER.splitToList(name);
                if (parts.size() == 4) {
                    return Optional.of("<galaxy:%s:%s:%s:%s>".formatted(parts.get(1), parts.get(2), owningRoleId, crypto.encryptFromUtf8(getGalaxyToken(identity))));
                }
            }
        }
        return Optional.empty();
    }

    private static String getGalaxyToken(Identity identity)
    {
        return requireNonNull(identity.getExtraCredentials().get(GALAXY_TOKEN_CREDENTIAL), "token is null");
    }

    private static boolean isIdentityEncodedInTheUserString(Identity identity)
    {
        return GALAXY_USER_STRING_IDENTITY_MATCHER.matcher(identity.getUser()).matches();
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
