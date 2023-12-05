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

import com.google.inject.Inject;
import io.trino.server.galaxy.catalogs.LiveCatalogsGalaxyAccessControllerSupplier;
import io.trino.spi.security.SystemAccessControl;
import io.trino.spi.security.SystemAccessControlFactory;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class GalaxyLiveCatalogsSystemAccessFactory
        implements SystemAccessControlFactory
{
    public static final String NAME = "galaxy-live";
    private final int backgroundProcessingThreads;
    private final LiveCatalogsGalaxyAccessControllerSupplier liveCatalogsGalaxyAccessControllerSupplier;

    @Inject
    public GalaxyLiveCatalogsSystemAccessFactory(GalaxySystemAccessControlConfig systemAccessControlConfig, LiveCatalogsGalaxyAccessControllerSupplier liveCatalogsGalaxyAccessControllerSupplier)
    {
        backgroundProcessingThreads = requireNonNull(systemAccessControlConfig, "systemAccessControlConfig is null").getBackgroundProcessingThreads();
        this.liveCatalogsGalaxyAccessControllerSupplier = requireNonNull(liveCatalogsGalaxyAccessControllerSupplier, "liveCatalogsGalaxyAccessControllerSupplier is null");
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public SystemAccessControl create(Map<String, String> ignore)
    {
        return new GalaxyAccessControl(backgroundProcessingThreads, liveCatalogsGalaxyAccessControllerSupplier);
    }
}
