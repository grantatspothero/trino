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
package io.trino.plugin.iceberg.catalog.meteor;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import java.net.URI;

public class MeteorCatalogConfig
{
    private URI catalogURI;

    public URI getCatalogUri()
    {
        return this.catalogURI;
    }

    @Config("iceberg.meteor-catalog.uri")
    @ConfigDescription("The URI to the Meteor state server")
    public MeteorCatalogConfig setCatalogUri(String uri)
    {
        if (uri != null) {
            this.catalogURI = URI.create(uri);
        }
        return this;
    }
}
