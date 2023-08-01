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
package io.trino.connector.informationschema.galaxy;

import com.google.inject.Inject;
import io.trino.FullConnectorSession;
import io.trino.connector.informationschema.InformationSchemaPageSource;
import io.trino.connector.informationschema.InformationSchemaPageSourceProvider;
import io.trino.connector.informationschema.InformationSchemaTableHandle;
import io.trino.metadata.Metadata;
import io.trino.security.AccessControl;
import io.trino.spi.connector.ColumnHandle;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class GalaxyCacheInformationSchemaPageSourceProvider
        extends InformationSchemaPageSourceProvider
{
    private final Metadata metadata;
    private final AccessControl accessControl;
    private final GalaxyCacheClient galaxyCacheClient;

    @Inject
    public GalaxyCacheInformationSchemaPageSourceProvider(Metadata metadata, AccessControl accessControl, GalaxyCacheClient galaxyCacheClient)
    {
        super(metadata, accessControl);

        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.galaxyCacheClient = requireNonNull(galaxyCacheClient, "galaxyCacheClient is null");
    }

    @Override
    protected InformationSchemaPageSource buildInformationSchemaPageSource(FullConnectorSession session, InformationSchemaTableHandle tableHandle, List<ColumnHandle> columns)
    {
        return new GalaxyCacheInformationSchemaPageSource(galaxyCacheClient, session.getSession(), metadata, accessControl, tableHandle, columns);
    }
}
