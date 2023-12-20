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
package io.trino.plugin.dynamodb;

import com.google.inject.Binder;
import com.google.inject.BindingAnnotation;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.starburstdata.trino.plugin.dynamodb.DynamoDbConfig;
import com.starburstdata.trino.plugin.dynamodb.DynamoDbConnectionFactory;
import com.starburstdata.trino.plugin.dynamodb.DynamoDbModule;
import com.starburstdata.trino.plugin.license.LicenseManager;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.RetryingConnectionFactory;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.jmx.StatisticsAwareConnectionFactory;
import io.trino.spi.connector.CatalogHandle;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static java.util.Objects.requireNonNull;

public class GalaxyDynamoDbModule
        extends AbstractConfigurationAwareModule
{
    private final LicenseManager licenseManager;

    public GalaxyDynamoDbModule(LicenseManager licenseManager)
    {
        this.licenseManager = requireNonNull(licenseManager, "licenseManager is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        createDir(new DynamoDbConfig().getSchemaDirectory());
        install(new DynamoDbModule(licenseManager));
        newOptionalBinder(binder, Key.get(ConnectionFactory.class, ForBaseJdbc.class))
                .setBinding()
                .to(Key.get(ConnectionFactory.class, ForGalaxy.class))
                .in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @ForGalaxy
    public ConnectionFactory getConnectionFactory(DynamoDbConfig dynamoDbConfig, CredentialProvider credentialProvider, CatalogHandle catalogHandle)
    {
        String modifiedSchemaDir = dynamoDbConfig.getSchemaDirectory() + "/%s".formatted(catalogHandle.getId());
        createDir(modifiedSchemaDir);
        dynamoDbConfig.setSchemaDirectory(modifiedSchemaDir);

        // The CData JDBC driver will intermittently throw an exception with no error message
        // Typically, retrying the query will cause it to proceed
        return new RetryingConnectionFactory(
                new StatisticsAwareConnectionFactory(
                        new DynamoDbConnectionFactory(dynamoDbConfig, credentialProvider)),
                new RetryingConnectionFactory.DefaultRetryStrategy());
    }

    private static void createDir(String absoluteDirPath)
    {
        Path path = Paths.get(absoluteDirPath);
        try {
            Files.createDirectories(path);
        }
        catch (IOException e) {
            throw new RuntimeException("Initialization failed. Cannot create %s directory.".formatted(absoluteDirPath), e);
        }
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @BindingAnnotation
    public @interface ForGalaxy {}
}
