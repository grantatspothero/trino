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

import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.starburst.stargate.crypto.KmsCryptoModule;
import io.starburst.stargate.crypto.MasterKeyCrypto;
import io.starburst.stargate.crypto.SecretSealer;
import io.trino.server.metadataonly.MetadataOnlyConfig;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.starburst.stargate.crypto.TestingMasterKeyCrypto.createVerifierCrypto;

public class MetadataOnlyGalaxyCatalogInfoSupplierModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(MetadataOnlyConfig.class);
        MetadataOnlyConfig metadataOnlyConfig = buildConfigObject(MetadataOnlyConfig.class);
        binder.bind(SecretSealer.class).in(SINGLETON);
        if (metadataOnlyConfig.getUseKmsCrypto()) {
            install(new KmsCryptoModule());
        }
        else {
            // TODO change to metadata path
            binder.bind(MasterKeyCrypto.class).toInstance(createVerifierCrypto(metadataOnlyConfig.getTrinoPlaneId().toString()));
        }
        binder.bind(GalaxyCatalogInfoSupplier.class).to(MetadataOnlyEncryptedGalaxyCatalogInfoSupplier.class);
    }
}
