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
package io.trino.plugin.base.galaxy;

import io.starburst.stargate.id.CatalogId;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static io.trino.plugin.base.galaxy.GalaxyCatalogVersionUtils.fromCatalogId;
import static io.trino.plugin.base.galaxy.GalaxyCatalogVersionUtils.fromCatalogIdAndLiveCatalogUniqueId;
import static io.trino.plugin.base.galaxy.GalaxyCatalogVersionUtils.fromCatalogIdAndTransactionId;
import static io.trino.spi.connector.CatalogHandle.CatalogVersion;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCatalogHandleVersion
{
    @Test
    public void testCatalogVersionStatic()
    {
        CatalogId catalogId = new CatalogId("c-5043666763");
        CatalogVersion version = fromCatalogId(catalogId);
        GalaxyCatalogVersionUtils.GalaxyCatalogVersion galaxyVersion = GalaxyCatalogVersionUtils.GalaxyCatalogVersion.fromCatalogVersion(version);
        assertThat(galaxyVersion.catalogId()).isEqualTo(catalogId);
        assertThat(galaxyVersion.transactionId()).isNotPresent();
        assertThat(galaxyVersion.liveCatalogUniqueId()).isNotPresent();
    }

    @Test
    public void testCatalogVersionLive()
    {
        CatalogId catalogId = new CatalogId("c-5043666763");
        UUID uniqueId = UUID.randomUUID();
        CatalogVersion version = fromCatalogIdAndLiveCatalogUniqueId(catalogId, uniqueId);
        GalaxyCatalogVersionUtils.GalaxyCatalogVersion galaxyVersion = GalaxyCatalogVersionUtils.GalaxyCatalogVersion.fromCatalogVersion(version);
        assertThat(galaxyVersion.catalogId()).isEqualTo(catalogId);
        assertThat(galaxyVersion.transactionId()).isNotPresent();
        assertThat(galaxyVersion.liveCatalogUniqueId()).hasValue(uniqueId);
    }

    @Test
    public void testCatalogVersionMetadata()
    {
        CatalogId catalogId = new CatalogId("c-5043666763");
        UUID transactionId = UUID.randomUUID();
        CatalogVersion version = fromCatalogIdAndTransactionId(catalogId, transactionId);
        GalaxyCatalogVersionUtils.GalaxyCatalogVersion galaxyVersion = GalaxyCatalogVersionUtils.GalaxyCatalogVersion.fromCatalogVersion(version);
        assertThat(galaxyVersion.catalogId()).isEqualTo(catalogId);
        assertThat(galaxyVersion.transactionId()).hasValue(transactionId);
        assertThat(galaxyVersion.liveCatalogUniqueId()).isNotPresent();
    }
}
