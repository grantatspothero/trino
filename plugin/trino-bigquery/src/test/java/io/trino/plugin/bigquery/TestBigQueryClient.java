package io.trino.plugin.bigquery;

import com.google.common.cache.CacheBuilder;
import io.trino.plugin.base.cache.EvictableCache;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;
import static org.testng.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class TestBigQueryClient {

    @Test
    public void testMappingWithEmptyCacheWithNonExistentCacheKey()
    {
        EvictableCache<String, Optional<String>> cache = EvictableCache.buildWith(CacheBuilder.newBuilder());

        BigQueryClient.refreshCacheFromLocalPresentMapping(cache, "someNonExistentName", () -> {
            Map<String, String> presentMapping = new HashMap<>();
            presentMapping.put("beforeName", "afterName");
            presentMapping.put("before", "after");
            return presentMapping;
        });

        // The cache is filled up with:
        // - positive cache entries
        // - single negative cache entry that was explicitly requested
        assertCacheEqualsMap(cache, ImmutableMap.of("beforeName", Optional.of("afterName"), "before", Optional.of("after"), "someNonExistentName", Optional.empty()));
    }

    @Test
    public void testMappingWithEmptyCacheWithExistentCacheKey()
    {
        EvictableCache<String, Optional<String>> cache = EvictableCache.buildWith(CacheBuilder.newBuilder());

        BigQueryClient.refreshCacheFromLocalPresentMapping(cache, "beforeName", () -> {
            Map<String, String> presentMapping = new HashMap<>();
            presentMapping.put("beforeName", "afterName");
            presentMapping.put("before", "after");
            return presentMapping;
        });

        // The cache is filled up with:
        // - positive cache entries
        assertCacheEqualsMap(cache, ImmutableMap.of("beforeName", Optional.of("afterName"), "before", Optional.of("after")));
    }

    @Test
    public void testMappingWithNegativeCacheWithExistentCacheKey() throws ExecutionException
    {
        EvictableCache<String, Optional<String>> cache = EvictableCache.buildWith(CacheBuilder.newBuilder());
        cache.get("negativeCache", Optional::empty);

        BigQueryClient.refreshCacheFromLocalPresentMapping(cache, "beforeName", () -> {
            Map<String, String> presentMapping = new HashMap<>();
            presentMapping.put("beforeName", "afterName");
            presentMapping.put("before", "after");
            return presentMapping;
        });

        // The cache is filled up with:
        // - positive cache entries
        // - the original negative cache entry
        assertCacheEqualsMap(cache, ImmutableMap.of("beforeName", Optional.of("afterName"), "before", Optional.of("after"), "negativeCache", Optional.empty()));
    }

    @Test
    public void testMappingWithPositiveCacheWithExistentCacheKey() throws ExecutionException
    {
        EvictableCache<String, Optional<String>> cache = EvictableCache.buildWith(CacheBuilder.newBuilder());
        cache.get("before", () -> Optional.of("after"));

        BigQueryClient.refreshCacheFromLocalPresentMapping(cache, "beforeName", () -> {
            Map<String, String> presentMapping = new HashMap<>();
            presentMapping.put("beforeName", "afterName");
            presentMapping.put("before", "newAfter");
            return presentMapping;
        });

        // The previous cache entry is invalidated
        assertCacheEqualsMap(cache, ImmutableMap.of("beforeName", Optional.of("afterName"), "before", Optional.of("newAfter")));
    }

    @Test
    public void testMappingWithPositiveCacheWithExistentCacheKeyWithNewMapping() throws ExecutionException
    {
        EvictableCache<String, Optional<String>> cache = EvictableCache.buildWith(CacheBuilder.newBuilder());
        cache.get("before", () -> Optional.empty());

        BigQueryClient.refreshCacheFromLocalPresentMapping(cache, "beforeName", () -> {
            Map<String, String> presentMapping = new HashMap<>();
            presentMapping.put("beforeName", "afterName");
            presentMapping.put("before", "newAfter");
            return presentMapping;
        });

        // The previous cache entry is invalidated
        assertCacheEqualsMap(cache, ImmutableMap.of("beforeName", Optional.of("afterName"), "before", Optional.of("newAfter")));
    }

    private static <K,V> void assertCacheEqualsMap(EvictableCache<K,V> cache, Map<K,V> expectedMap)
    {
        // Cannot use standard equals on maps because EvictableCache.asMap() does not implement all required methods
        Map<K,V> actualMap = cache.asMap();
        assertEquals(actualMap.keySet(), expectedMap.keySet());
        for(Map.Entry<K,V> expectedEntry: expectedMap.entrySet()){
            assertEquals(actualMap.get(expectedEntry.getKey()), expectedEntry.getValue());
        }
    }
}
