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
package io.trino.server.galaxy.catalogs;

import com.google.inject.Inject;
import io.starburst.stargate.accesscontrol.client.TrinoSecurityApi;
import io.trino.server.galaxy.GalaxyPermissionsCache;
import io.trino.server.security.galaxy.GalaxyAccessControllerSupplier;
import io.trino.server.security.galaxy.GalaxySystemAccessController;
import io.trino.spi.security.Identity;

import static io.trino.server.security.galaxy.MetadataAccessControllerSupplier.extractTransactionId;
import static java.util.Objects.requireNonNull;

public class LiveCatalogsGalaxyAccessControllerSupplier
        implements GalaxyAccessControllerSupplier
{
    private final TrinoSecurityApi accessControlClient;
    private final CatalogResolver catalogResolver;
    private final GalaxyPermissionsCache galaxyPermissionsCache;

    @Inject
    public LiveCatalogsGalaxyAccessControllerSupplier(TrinoSecurityApi accessControlClient, CatalogResolver catalogResolver, GalaxyPermissionsCache galaxyPermissionsCache)
    {
        this.accessControlClient = requireNonNull(accessControlClient, "accessControlClient is null");
        this.catalogResolver = requireNonNull(catalogResolver, "catalogResolver is null");
        this.galaxyPermissionsCache = requireNonNull(galaxyPermissionsCache, "galaxyPermissionsCache is null");
    }

    @Override
    public GalaxySystemAccessController apply(Identity identity)
    {
        return new GalaxySystemAccessController(accessControlClient, catalogResolver, galaxyPermissionsCache, extractTransactionId(identity));
    }
}
