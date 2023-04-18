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
package io.trino.testing;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Module;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.jaxrs.JaxrsBinder;
import io.starburst.stargate.accesscontrol.client.testing.TestingAccountClient;
import io.starburst.stargate.id.AccountId;
import io.starburst.stargate.id.CatalogId;
import io.starburst.stargate.id.RoleId;
import io.starburst.stargate.id.UserId;
import io.starburst.stargate.identity.DispatchSession;
import io.trino.server.galaxy.GalaxySecurityModule;
import io.trino.server.security.InternalPrincipal;
import io.trino.server.security.galaxy.GalaxyIdentity.GalaxyIdentityType;
import io.trino.server.security.galaxy.GalaxyTrinoSystemAccessFactory;
import io.trino.spi.Plugin;
import io.trino.spi.security.Identity;

import javax.annotation.Priority;
import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.server.HttpRequestSessionContextFactory.AUTHENTICATED_IDENTITY;
import static io.trino.server.ServletSecurityUtils.setAuthenticatedIdentity;
import static io.trino.server.security.galaxy.GalaxyIdentity.createIdentity;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static javax.ws.rs.core.Response.Status.FORBIDDEN;

/**
 * Query runner to run plugins as they would execute in a Galaxy context (galaxy permissions set up and enabled)
 */
public final class GalaxyQueryRunner
{
    private GalaxyQueryRunner() {}

    public static Builder builder()
    {
        return new Builder(null, null);
    }

    public static Builder builder(String defaultSessionCatalog, String defaultSessionSchema)
    {
        return new Builder(defaultSessionCatalog, defaultSessionSchema);
    }

    public static final class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private record CatalogInit(
                String catalogName,
                String connectorName,
                Map<String, String> properties)
        {
            CatalogInit {
                requireNonNull(catalogName, "catalogName is null");
                requireNonNull(connectorName, "connectorName is null");
                properties = ImmutableMap.copyOf(requireNonNull(properties, "properties is null"));
            }
        }

        private TestingAccountClient accountClient;
        private final ImmutableList.Builder<Plugin> plugins = ImmutableList.builder();
        private final ImmutableList.Builder<CatalogInit> catalogs = ImmutableList.builder();
        private String httpAuthenticationType = "galaxy";
        private boolean installSecurityModule = true;

        private Builder(String defaultSessionCatalog, String defaultSessionSchema)
        {
            super(testSessionBuilder()
                    .setIdentity(Identity.ofUser("galaxy_to_be_overwritten"))
                    .setCatalog(defaultSessionCatalog)
                    .setSchema(defaultSessionSchema)
                    .build());
        }

        public Builder setAccountClient(TestingAccountClient accountClient)
        {
            this.accountClient = requireNonNull(accountClient, "accountClient is null");
            return this;
        }

        @Override
        public Builder setAdditionalModule(Module additionalModule)
        {
            // this could be supported but we don't need it
            throw new UnsupportedOperationException("Additional modules are hard coded");
        }

        public Builder addPlugin(Plugin plugin)
        {
            this.plugins.add(plugin);
            return self();
        }

        public Builder addCatalog(String catalogName, String connectorName, Map<String, String> properties)
        {
            this.catalogs.add(new CatalogInit(catalogName, connectorName, properties));
            return self();
        }

        public Builder setHttpAuthenticationType(String httpAuthenticationType)
        {
            this.httpAuthenticationType = requireNonNull(httpAuthenticationType, "httpAuthenticationType is null");
            return self();
        }

        public Builder setInstallSecurityModule(boolean installSecurityModule)
        {
            this.installSecurityModule = installSecurityModule;
            return self();
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            checkState(accountClient != null, "accountClient not set");

            URI accountUri = accountClient.getBaseUri();
            AccountId accountId = accountClient.getAccountId();
            URI deploymentUri = URI.create("https://test-sample.trino.local.gate0.net:8888");

            Set<String> catalogNames = catalogs.build().stream().map(CatalogInit::catalogName).collect(toImmutableSet());
            Map<String, CatalogId> catalogIds = catalogNames.stream().collect(toImmutableMap(Function.identity(), accountClient::createCatalog));
            String catalogIdStrings = catalogIds.entrySet().stream()
                    .map(entry -> entry.getKey() + "->" + entry.getValue())
                    .collect(joining(","));

            addExtraProperty("http-server.authentication.type", httpAuthenticationType);
            addExtraProperty("galaxy.authentication.token-issuer", deploymentUri.toString());
            addExtraProperty("galaxy.authentication.public-key", accountClient.getSampleDeploymentPublicKey());
            addExtraProperty("query.info-url-template", deploymentUri + "/ui/query.html?${QUERY_ID}");
            addExtraProperty("legacy.allow-set-view-authorization", "TRUE");

            amendSession(sessionBuilder -> sessionBuilder.setIdentity(createIdentity(
                    accountClient.getAdminEmail(),
                    accountId,
                    accountClient.getAdminUserId(),
                    accountClient.getAdminRoleId(),
                    accountClient.getAdminTrinoAccessToken(),
                    GalaxyIdentityType.DEFAULT)));

            if (installSecurityModule) {
                addExtraProperty("galaxy.account-id", accountId.toString());
                addExtraProperty("galaxy.deployment-id", accountClient.getSampleDeploymentId().toString());
                addExtraProperty("galaxy.account-url", accountUri.toString());
                addExtraProperty("galaxy.catalog-names", catalogIdStrings);
                addExtraProperty("galaxy.cluster-id", accountClient.getSampleClusterId().toString());

                setSystemAccessControl(new GalaxyTrinoSystemAccessFactory()
                        .create(ImmutableMap.<String, String>builder()
                                .put("galaxy.account-url", accountUri.toString())
                                .put("galaxy.catalog-names", catalogIdStrings)
                                .buildOrThrow()));
            }

            super.setAdditionalModule(new AbstractConfigurationAwareModule()
            {
                @Override
                protected void setup(Binder binder)
                {
                    if (installSecurityModule) {
                        install(new GalaxySecurityModule());
                    }
                    binder.bind(TestingAccountClient.class).toInstance(accountClient);
                    JaxrsBinder.jaxrsBinder(binder).bind(TestingGalaxyIdentityFilter.class);
                }
            });

            DistributedQueryRunner queryRunner = super.build();

            try {
                for (Plugin plugin : plugins.build()) {
                    queryRunner.installPlugin(plugin);
                }
                for (CatalogInit catalogInit : catalogs.build()) {
                    queryRunner.createCatalog(catalogInit.catalogName(), catalogInit.connectorName(), catalogInit.properties());
                }

                return queryRunner;
            }
            catch (Exception e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }

    /**
     * This query runner does not use the Galaxy authentication system, so the identity is not setup property.
     * This filter process there request after the Trino "insecure" authentication filter, and builds a
     * proper Galaxy identity so the downstream Galaxy services (e.g., authorization) work correctly.
     */
    @Priority(Priorities.AUTHENTICATION + 1)
    public static class TestingGalaxyIdentityFilter
            implements ContainerRequestFilter
    {
        private final TestingAccountClient accountClient;

        @Inject
        public TestingGalaxyIdentityFilter(TestingAccountClient accountClient)
        {
            this.accountClient = requireNonNull(accountClient, "accountClient is null");
        }

        @Override
        public void filter(ContainerRequestContext request)
                throws IOException
        {
            if (request.getSecurityContext().getUserPrincipal() instanceof InternalPrincipal) {
                return;
            }

            Identity existingIdentity = (Identity) request.getProperty(AUTHENTICATED_IDENTITY);
            if (existingIdentity == null) {
                return;
            }

            // extra credentials are added to identity inside of query submission, so manually process extra credentials from the headers
            Map<String, String> extraCredentials = Optional.ofNullable(request.getHeaders().get("X-Trino-Extra-Credential")).orElseThrow(() -> new BadRequestException("Request expected to have X-Trino-Extra-Credential header"))
                    .stream()
                    .map(header -> Splitter.on('=').trimResults().splitToList(header))
                    .filter(parts -> parts.size() == 2)
                    .collect(toImmutableMap(parts -> parts.get(0), parts -> parts.get(1)));
            request.getHeaders().remove("X-Trino-Extra-Credential");

            DispatchSession dispatchSession = accountClient.createDispatchSession(
                    new UserId(getRequiredExtraCredential(extraCredentials, "userId")),
                    new RoleId(getRequiredExtraCredential(extraCredentials, "roleId")));

            Identity identity = createIdentity(
                    existingIdentity.getUser(),
                    dispatchSession.getAccountId(),
                    dispatchSession.getUserId(),
                    dispatchSession.getRoleId(),
                    dispatchSession.getAccessToken(),
                    GalaxyIdentityType.DEFAULT);

            setAuthenticatedIdentity(request, identity);
        }

        private static String getRequiredExtraCredential(Map<String, String> extraCredentials, String name)
        {
            String value = extraCredentials.get(name);
            if (value == null) {
                throw new ForbiddenException(Response.status(FORBIDDEN).entity("Extra credential missing: " + name).build());
            }
            return value;
        }
    }
}
