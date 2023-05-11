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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableSet;
import io.starburst.stargate.accesscontrol.client.TrinoSecurityApi;
import io.starburst.stargate.accesscontrol.client.testing.TestingAccountClient;
import io.starburst.stargate.id.AccountId;
import io.starburst.stargate.id.CatalogId;
import io.starburst.stargate.id.RoleId;
import io.starburst.stargate.id.RoleName;
import io.starburst.stargate.id.UserId;
import io.starburst.stargate.identity.DispatchSession;
import io.trino.Session;
import io.trino.server.galaxy.GalaxyCockroachContainer;
import io.trino.server.galaxy.GalaxyPermissionsCache;
import io.trino.server.security.galaxy.GalaxyIdentity.GalaxyIdentityType;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.Identity;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.testing.TestingSession;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.collect.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.server.security.galaxy.GalaxyIdentity.createIdentity;
import static io.trino.server.security.galaxy.GalaxyIdentity.createPrincipalString;
import static io.trino.server.security.galaxy.GalaxyIdentity.toDispatchSession;
import static io.trino.server.security.galaxy.TestingAccountFactory.createTestingAccountFactory;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class GalaxyTestHelper
        implements AutoCloseable
{
    public static final String ACCOUNT_ADMIN = "accountadmin";
    public static final String PUBLIC = "public";
    public static final String FEARLESS_LEADER = "fearless_leader";
    public static final String LACKEY_FOLLOWER = "lackey_follower";

    private GalaxyCockroachContainer cockroach;
    private TestingAccountFactory accountFactory;
    private GalaxySystemAccessController accessController;
    private TestingAccountClient accountClient;
    private GalaxyAccessControl accessControl;
    private GalaxySecurityMetadata metadataApi;
    private TrinoSecurityApi client;
    private CatalogIds catalogIds;

    private final AtomicInteger queryIds = new AtomicInteger();

    private LoadingCache<UserIdAndRoleId, DispatchSession> dispatchSessionCache;

    public void initialize()
            throws Exception
    {
        cockroach = new GalaxyCockroachContainer();
        accountFactory = createTestingAccountFactory(() -> cockroach);
        accountClient = accountFactory.createAccountClient();

        // creating auth keys is very slow so cache them
        // todo figure out why this is slow
        dispatchSessionCache = buildNonEvictableCache(
                CacheBuilder.newBuilder(),
                CacheLoader.from(userIdAndRoleId -> accountClient.createDispatchSession(userIdAndRoleId.userId(), userIdAndRoleId.roleId())));

        Map<String, CatalogId> catalogs = IntStream.range(1, 5 + 1)
                .mapToObj(index -> "catalog" + index)
                .collect(toImmutableMap(Function.identity(), accountClient::createCatalog));

        catalogIds = new CatalogIds(ImmutableBiMap.copyOf(catalogs), ImmutableSet.of());
        client = accountClient.getTrinoSecurityApi();
        GalaxyPermissionsCache permissionsCache = new GalaxyPermissionsCache();
        accessController = new GalaxySystemAccessController(client, catalogIds, permissionsCache);
        accessControl = new GalaxyAccessControl(ignore -> accessController);
        metadataApi = new GalaxySecurityMetadata(client, catalogIds, permissionsCache);

        // Make the roles
        metadataApi.createRole(adminSession(), FEARLESS_LEADER, Optional.empty());
        metadataApi.createRole(adminSession(), LACKEY_FOLLOWER, Optional.empty());
        metadataApi.grantRoles(adminSession(), ImmutableSet.of(LACKEY_FOLLOWER), ImmutableSet.of(new TrinoPrincipal(PrincipalType.ROLE, FEARLESS_LEADER)), false, Optional.empty());
    }

    @Override
    public void close()
            throws Exception
    {
        if (cockroach != null) {
            cockroach.close();
            cockroach = null;
        }
        if (accountFactory != null) {
            accountFactory.close();
        }
    }

    public GalaxyCockroachContainer getCockroach()
    {
        return cockroach;
    }

    public TestingAccountClient getAccountClient()
    {
        return accountClient;
    }

    public GalaxySystemAccessController getAccessController()
    {
        return accessController;
    }

    public GalaxyAccessControl getAccessControl()
    {
        return accessControl;
    }

    public TrinoSecurityApi getClient()
    {
        return client;
    }

    public GalaxySecurityMetadata getMetadataApi()
    {
        return metadataApi;
    }

    public CatalogIds getCatalogIds()
    {
        return catalogIds;
    }

    public CatalogId getCatalogId(String catalogName)
    {
        return catalogIds.getCatalogId(catalogName).orElseThrow(() -> new IllegalArgumentException("Unknown catalog " + catalogName));
    }

    public SystemSecurityContext context(RoleId roleId)
    {
        return context(session(roleId));
    }

    public Map<RoleName, RoleId> getActiveRoles(SystemSecurityContext context)
    {
        return accessController.listEnabledRoles(withNewQueryId(context));
    }

    public void checkAccess(String message, List<SystemSecurityContext> successfulContexts, List<SystemSecurityContext> failingContexts, Consumer<SystemSecurityContext> consumer)
    {
        successfulContexts.forEach(context -> {
            try {
                consumer.accept(context);
            }
            catch (Throwable e) {
                handleUnexpectedResult("Failed unexpectedly", message, context, e);
            }
        });

        failingContexts.forEach(context -> {
            try {
                assertThatThrownBy(() -> consumer.accept(context))
                        .isInstanceOf(AccessDeniedException.class)
                        .hasMessage(message);
            }
            catch (Throwable e) {
                handleUnexpectedResult("Succeeded unexpectedly", message, context, e);
            }
        });
    }

    public void checkAccessMatching(String message, List<SystemSecurityContext> successfulContexts, List<SystemSecurityContext> failingContexts, Consumer<SystemSecurityContext> consumer)
    {
        successfulContexts.forEach(context -> {
            try {
                consumer.accept(context);
            }
            catch (Throwable e) {
                handleUnexpectedResult("Failed unexpectedly", message, context, e);
            }
        });

        failingContexts.forEach(context -> {
            try {
                assertThatThrownBy(() -> consumer.accept(context))
                        .isInstanceOf(AccessDeniedException.class)
                        .hasMessageMatching(message);
            }
            catch (Throwable e) {
                handleUnexpectedResult("Succeeded unexpectedly", message, context, e);
            }
        });
    }

    public void accessCausesTrinoException(String message, List<SystemSecurityContext> contexts, Consumer<SystemSecurityContext> consumer)
    {
        contexts.forEach(context -> {
            try {
                assertThatThrownBy(() -> consumer.accept(context))
                        .isInstanceOf(TrinoException.class)
                        .hasMessageMatching(message);
            }
            catch (Throwable e) {
                handleUnexpectedResult("Succeeded unexpectedly", message, context, e);
            }
        });
    }

    private void handleUnexpectedResult(String description, String message, SystemSecurityContext context, Throwable e)
    {
        Map<String, String> credentials = context.getIdentity().getExtraCredentials();
        RoleId roleId = new RoleId(credentials.get("roleId"));
        BiMap<RoleName, RoleId> roles = HashBiMap.create(client.listRoles(toDispatchSession(adminSession())));
        RoleName roleName = roles.inverse().get(roleId);
        throw new AssertionError(format("%s, expected message %s, roleName %s, roleId %s", description, message, roleName, roleId), e);
    }

    public Identity roleNameToIdentity(String roleName)
    {
        AccountId accountId = accountClient.getAccountId();
        UserId userId = accountClient.getAdminUserId();
        RoleId adminRoleId = accountClient.getAdminRoleId();
        Map<RoleName, RoleId> roles = client.listEnabledRoles(accountClient.createDispatchSession(userId, adminRoleId));
        RoleId roleId = requireNonNull(roles.get(new RoleName(roleName)), "roles.get(roleName) is null");
        String principal = createPrincipalString(accountId, userId, roleId);
        Set<String> enabledRoles = client.listEnabledRoles(accountClient.createDispatchSession(userId, roleId)).keySet().stream()
                .map(RoleName::getName)
                .collect(toImmutableSet());
        return new Identity.Builder("foo@bar.com")
                .withPrincipal(new BasicPrincipal(principal))
                .withEnabledRoles(enabledRoles)
                .build();
    }

    public Identity identity(UserId userId, RoleId roleId)
    {
        DispatchSession dispatchSession = dispatchSessionCache.getUnchecked(new UserIdAndRoleId(userId, roleId));

        Set<String> enabledRoles = client.listEnabledRoles(dispatchSession).keySet().stream()
                .map(RoleName::getName)
                .collect(toImmutableSet());

        return createIdentity(
                accountClient.getAdminEmail(),
                dispatchSession.getAccountId(),
                dispatchSession.getUserId(),
                dispatchSession.getRoleId(),
                enabledRoles,
                dispatchSession.getAccessToken(),
                GalaxyIdentityType.DEFAULT);
    }

    public Session session(UserId userId, RoleId roleId)
    {
        return TestingSession.testSessionBuilder()
                .setIdentity(identity(userId, roleId))
                .setQueryId(new QueryId(String.valueOf(queryIds.incrementAndGet())))
                .build();
    }

    public Session session(RoleId roleId)
    {
        return session(accountClient.getAdminUserId(), roleId);
    }

    public Session adminSession()
    {
        return withNewQueryId(session(accountClient.getAdminRoleId()));
    }

    public Session publicSession()
    {
        return withNewQueryId(session(accountClient.getPublicRoleId()));
    }

    public SystemSecurityContext context(Session session)
    {
        return new SystemSecurityContext(session.getIdentity(), Optional.of(new QueryId(String.valueOf(queryIds.incrementAndGet()))));
    }

    public SystemSecurityContext adminContext()
    {
        return withNewQueryId(context(session(accountClient.getAdminRoleId())));
    }

    public SystemSecurityContext publicContext()
    {
        return withNewQueryId(context(session(accountClient.getPublicRoleId())));
    }

    private Session withNewQueryId(Session session)
    {
        return TestingSession.testSessionBuilder()
                .setIdentity(session.getIdentity())
                .setQueryId(new QueryId(String.valueOf(queryIds.incrementAndGet())))
                .build();
    }

    private SystemSecurityContext withNewQueryId(SystemSecurityContext context)
    {
        return new SystemSecurityContext(context.getIdentity(), Optional.of(new QueryId(String.valueOf(queryIds.incrementAndGet()))));
    }

    public String getAnyCatalogName()
    {
        return catalogIds.getCatalogNames().stream()
                .findAny()
                .orElseThrow(() -> new IllegalStateException("Could not find a catalog name"));
    }

    private record UserIdAndRoleId(UserId userId, RoleId roleId)
    {
        private UserIdAndRoleId
        {
            requireNonNull(userId, "userId is null");
            requireNonNull(roleId, "roleId is nul");
        }
    }
}
