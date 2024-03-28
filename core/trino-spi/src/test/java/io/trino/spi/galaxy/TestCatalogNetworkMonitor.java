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
package io.trino.spi.galaxy;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static io.trino.spi.galaxy.CatalogNetworkMonitor.getCrossRegionCatalogNetworkMonitor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestCatalogNetworkMonitor
{
    private static final int CHUNK_SIZE = 64;
    private static final int MAX_CROSS_REGION_BYTES = CHUNK_SIZE * 2;

    @Test
    public void testBasicUsageMonitoring()
                throws IOException
    {
        for (int i = 0; i < 2; i++) {
            CatalogNetworkMonitor catalogNetworkMonitor = getCrossRegionCatalogNetworkMonitor("foocatalog", "c-1234567890", MAX_CROSS_REGION_BYTES, MAX_CROSS_REGION_BYTES);
            try (InputStream baseInputStream = new ByteArrayInputStream(new byte[CHUNK_SIZE])) {
                // create monitored input stream, read a certain amount of bytes, and verify that the amount read is correctly reported
                InputStream monitoredInputStream = catalogNetworkMonitor.monitorInputStream(CatalogConnectionType.CROSS_REGION, baseInputStream);

                monitoredInputStream.readNBytes(CHUNK_SIZE);
                long readBytes = catalogNetworkMonitor.getCrossRegionReadBytes();
                assertThat(readBytes).isEqualTo(CHUNK_SIZE * (i + 1L));
            }
        }

        assertThatCode(() -> getCrossRegionCatalogNetworkMonitor("foocatalog", "c-1234567890", MAX_CROSS_REGION_BYTES, MAX_CROSS_REGION_BYTES))
                .doesNotThrowAnyException();

        CatalogNetworkMonitor catalogNetworkMonitor = getCrossRegionCatalogNetworkMonitor("foocatalog", "c-1234567890", MAX_CROSS_REGION_BYTES, MAX_CROSS_REGION_BYTES);
        try (InputStream baseInputStream = new ByteArrayInputStream(new byte[CHUNK_SIZE])) {
            // create monitored input stream, read a certain amount of bytes, and verify that the amount read is correctly reported
            InputStream monitoredInputStream = catalogNetworkMonitor.monitorInputStream(CatalogConnectionType.CROSS_REGION, baseInputStream);

            assertThatThrownBy(() -> monitoredInputStream.readNBytes(CHUNK_SIZE))
                    .hasMessage("Cross-region read data transfer limit of 0GB per worker exceeded. To increase this limit, contact Starburst support.");
            long readBytes = catalogNetworkMonitor.getCrossRegionReadBytes();
            assertThat(readBytes).isEqualTo(CHUNK_SIZE * 3);
        }

        assertThatThrownBy(() -> getCrossRegionCatalogNetworkMonitor("foocatalog", "c-1234567890", MAX_CROSS_REGION_BYTES, MAX_CROSS_REGION_BYTES))
                .hasMessage("Cross-region read data transfer limit of 0GB per worker exceeded. To increase this limit, contact Starburst support.");
    }

    @Test
    public void testBasicPrivateLinkMonitoring()
            throws IOException
    {
        CatalogNetworkMonitor catalogNetworkMonitor = CatalogNetworkMonitor.getCatalogNetworkMonitor("privateLinkCatalog", "c-1234567891");
        for (int i = 0; i < 2; i++) {
            try (InputStream baseInputStream = new ByteArrayInputStream(new byte[CHUNK_SIZE])) {
                // create monitored input stream, read a certain amount of bytes, and verify that the amount read is correctly reported
                InputStream monitoredInputStream = catalogNetworkMonitor.monitorInputStream(CatalogConnectionType.PRIVATE_LINK, baseInputStream);
                monitoredInputStream.readNBytes(CHUNK_SIZE);
                long readBytes = catalogNetworkMonitor.getPrivateLinkReadBytes();
                assertThat(readBytes).isEqualTo(CHUNK_SIZE * (i + 1L));
            }
        }
    }
}
