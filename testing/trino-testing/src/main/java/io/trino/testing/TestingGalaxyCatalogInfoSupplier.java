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
package io.trino.testing;

import io.starburst.stargate.id.CatalogVersion;
import io.starburst.stargate.identity.DispatchSession;
import io.trino.server.galaxy.catalogs.GalaxyCatalogArgs;
import io.trino.server.galaxy.catalogs.GalaxyCatalogInfo;
import io.trino.server.galaxy.catalogs.GalaxyCatalogInfoSupplier;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static org.assertj.core.api.Assertions.assertThat;

public class TestingGalaxyCatalogInfoSupplier
        implements GalaxyCatalogInfoSupplier
{
    private Map<GalaxyCatalogArgs, GalaxyCatalogInfo> catalogInfoMap = new HashMap<>();
    private Map<GalaxyCatalogArgs, AtomicLong> getCalls = new ConcurrentHashMap<>();

    public void addCatalog(GalaxyCatalogArgs galaxyCatalogArgs, GalaxyCatalogInfo galaxyCatalogInfo)
    {
        catalogInfoMap.put(galaxyCatalogArgs, galaxyCatalogInfo);
    }

    public CatalogVersion getOnlyVersionForCatalogName(String catalogName)
    {
        Set<CatalogVersion> allCatalogsWithName = getAllCatalogsWithName(catalogName);
        assertThat(allCatalogsWithName.size()).withFailMessage("Expecting only one catalog with name %s, found %d", catalogName, allCatalogsWithName.size()).isEqualTo(1);
        return allCatalogsWithName.stream().findAny().get();
    }

    public Set<CatalogVersion> getAllCatalogsWithName(String catalogName)
    {
        return catalogInfoMap.entrySet()
                .stream()
                .filter(entry -> entry.getValue().catalogProperties().catalogHandle().getCatalogName().equals(catalogName))
                .map(entry -> entry.getKey().catalogVersion())
                .collect(toImmutableSet());
    }

    @Override
    public GalaxyCatalogInfo getGalaxyCatalogInfo(DispatchSession dispatchSession, GalaxyCatalogArgs galaxyCatalogArgs)
    {
        getCalls.computeIfAbsent(galaxyCatalogArgs, ignore -> new AtomicLong()).incrementAndGet();
        return catalogInfoMap.get(galaxyCatalogArgs);
    }
}
