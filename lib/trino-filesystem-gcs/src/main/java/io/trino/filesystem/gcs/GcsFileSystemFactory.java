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
package io.trino.filesystem.gcs;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;
import io.trino.cache.NonEvictableLoadingCache;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.gcs.galaxy.GcsRegionEnforcementConfig;
import io.trino.spi.security.ConnectorIdentity;
import jakarta.annotation.PreDestroy;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static java.lang.Math.toIntExact;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class GcsFileSystemFactory
        implements TrinoFileSystemFactory
{
    private static final int MAX_LOCATION_REGION_VALIDATION_RESULT_CACHE_SIZE = 10_000;

    private final int readBlockSizeBytes;
    private final long writeBlockSizeBytes;
    private final int pageSize;
    private final int batchSize;
    private final Optional<String> enforcedRegion;
    private final ListeningExecutorService executorService;
    private final GcsStorageFactory storageFactory;
    private final NonEvictableLoadingCache<LocationKey, Optional<String>> locationRegionValidationResultCache;

    @Inject
    public GcsFileSystemFactory(GcsFileSystemConfig config, GcsRegionEnforcementConfig regionEnforcementConfig, GcsStorageFactory storageFactory)
    {
        this.readBlockSizeBytes = toIntExact(config.getReadBlockSize().toBytes());
        this.writeBlockSizeBytes = config.getWriteBlockSize().toBytes();
        this.pageSize = config.getPageSize();
        this.batchSize = config.getBatchSize();
        this.enforcedRegion = regionEnforcementConfig.getEnforcedRegion();
        this.storageFactory = requireNonNull(storageFactory, "storageFactory is null");
        this.executorService = listeningDecorator(newCachedThreadPool(daemonThreadsNamed("trino-filesystem-gcs-%S")));
        this.locationRegionValidationResultCache = buildNonEvictableCache(
                CacheBuilder.newBuilder().maximumSize(MAX_LOCATION_REGION_VALIDATION_RESULT_CACHE_SIZE),
                CacheLoader.from(locationKey -> validateRegion(locationKey.identity(), locationKey.host())));
    }

    @PreDestroy
    public void stop()
    {
        executorService.shutdownNow();
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity)
    {
        return new GcsFileSystem(executorService, storageFactory.create(identity), readBlockSizeBytes, writeBlockSizeBytes, pageSize, batchSize);
    }

    @Override
    public Optional<String> validate(ConnectorIdentity identity, Location location)
    {
        if (enforcedRegion.isEmpty()) {
            return Optional.empty();
        }

        try {
            return locationRegionValidationResultCache.get(createLocationKey(identity, location));
        }
        catch (ExecutionException e) {
            throw new RuntimeException("Error while validating regions for location %s".formatted(location), e);
        }
    }

    private Optional<String> validateRegion(ConnectorIdentity identity, String host)
    {
        if (enforcedRegion.isEmpty()) {
            return Optional.empty();
        }

        Bucket bucket = getGcsBucket(identity, host);
        Set<String> regions;
        if (Objects.equals(bucket.getLocationType(), "region")) {
            regions = ImmutableSet.of(bucket.getLocation());
        }
        else if (Objects.equals(bucket.getLocationType(), "dual-region")) {
            String dualRegionName = bucket.getLocation();
            regions = mapGcsDualRegions(dualRegionName);
            if (regions.isEmpty()) {
                return Optional.of("GCS dual-region bucket location %s not supported".formatted(dualRegionName));
            }
        }
        else {
            return Optional.of("GCS bucket location type '%s' not supported".formatted(bucket.getLocationType()));
        }

        regions = regions.stream()
                .map(GcsFileSystemFactory::normalizeGcsRegionName)
                .collect(toImmutableSet());
        if (!regions.contains(normalizeGcsRegionName(enforcedRegion.get()))) {
            return Optional.of("Google Cloud Storage bucket %s is in regions %s, but only region %s is allowed".formatted(host, regions, enforcedRegion.get()));
        }
        return Optional.empty();
    }

    private Bucket getGcsBucket(ConnectorIdentity identity, String bucketName)
    {
        try (Storage storage = storageFactory.create(identity)) {
            return storage.get(bucketName);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Set<String> mapGcsDualRegions(String dualRegionName)
    {
        // Mapping data populated from https://cloud.google.com/storage/docs/locations
        // TODO: Pass these hard-coded values through configs https://github.com/starburstdata/stargate/issues/15578
        return switch (dualRegionName) {
            case "ASIA1" -> ImmutableSet.of("asia-northeast1", "asia-northeast2");
            case "EUR4" -> ImmutableSet.of("europe-north1", "europe-west4");
            case "NAM4" -> ImmutableSet.of("us-central1", "us-east1");
            default -> ImmutableSet.of();
        };
    }

    private static String normalizeGcsRegionName(String regionName)
    {
        return regionName.toLowerCase(ENGLISH);
    }

    private static LocationKey createLocationKey(ConnectorIdentity identity, Location location)
    {
        String host = location.host()
                .orElseThrow(() -> new IllegalArgumentException("Missing host from the location %s".formatted(location)))
                .toLowerCase(ENGLISH);
        return new LocationKey(identity, host);
    }

    private record LocationKey(ConnectorIdentity identity, String host)
    {
        private LocationKey
        {
            requireNonNull(identity, "identity is null");
            requireNonNull(host, "host is null");
        }
    }
}
