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
package io.trino.plugin.salesforce;

import com.starburstdata.trino.plugin.license.LicenseManager;
import com.starburstdata.trino.plugin.salesforce.EnableWrites;
import com.starburstdata.trino.plugin.salesforce.SalesforceModule;
import io.trino.plugin.jdbc.JdbcConnectorFactory;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

import java.util.List;

import static io.airlift.configuration.ConfigurationAwareModule.combine;

public class GalaxySalesforcePlugin
        implements Plugin
{
    public GalaxySalesforcePlugin() {}

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return List.of(new JdbcConnectorFactory(
                "salesforce",
                combine(
                        binder -> binder.bind(Boolean.class).annotatedWith(EnableWrites.class).toInstance(false),
                        new SalesforceModule(new NoopLicenseManager()))));
    }

    private static class NoopLicenseManager
            implements LicenseManager
    {
        @Override
        public boolean hasLicense()
        {
            return true;
        }
    }
}
