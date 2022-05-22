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

import io.trino.spi.security.SystemAccessControl;
import io.trino.spi.security.SystemAccessControlFactory;

import javax.inject.Inject;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class GalaxyMetadataSystemAccessFactory
        implements SystemAccessControlFactory
{
    public static final String NAME = "galaxy-metadata";

    private final MetadataAccessControllerSupplier controllerSupplier;

    @Inject
    public GalaxyMetadataSystemAccessFactory(MetadataAccessControllerSupplier controller)
    {
        this.controllerSupplier = requireNonNull(controller, "controller is null");
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public SystemAccessControl create(Map<String, String> ignore)
    {
        return new GalaxyAccessControl(controllerSupplier);
    }
}
