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

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.net.InetAddresses.toAddrString;
import static io.trino.collect.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.plugin.base.galaxy.InetAddresses.toInetAddresses;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RegionVerifier
{
    // Use static caches to retain state because socket factories are unfortunately recreated for each socket
    private static final LoadingCache<List<String>, IpRangeMatcher> IP_RANGE_MATCHER_CACHE =
            buildNonEvictableCache(CacheBuilder.newBuilder(), CacheLoader.from(IpRangeMatcher::create));

    private static final String CROSS_REGION_ALLOWED_PROPERTY_NAME = "crossRegionAllowed";
    private static final String REGION_LOCAL_IP_ADDRESSES_PROPERTY_NAME = "regionLocalIpAddresses";
    private final IpRangeMatcher localIpRangeMatcher;
    private final boolean crossRegionAllowed;

    public RegionVerifier(Properties properties)
    {
        this(isCrossRegionAllowed(properties), getRegionLocalIpAddresses(requireNonNull(properties, "properties is null")));
    }

    public RegionVerifier(boolean crossRegionAllowed, List<String> allowedIpAddresses)
    {
        this.localIpRangeMatcher = IP_RANGE_MATCHER_CACHE.getUnchecked(requireNonNull(allowedIpAddresses, "allowedIpAddresses is null"));
        this.crossRegionAllowed = crossRegionAllowed;
    }

    public void verifyLocalRegion(String serverType, String host)
    {
        for (InetAddress inetAddress : toInetAddresses(host)) {
            isValidLocalIp(serverType, inetAddress);
        }
    }

    public boolean isCrossRegionAccess(String serverType, List<InetAddress> addresses)
    {
        for (InetAddress inetAddress : addresses) {
            if (isValidLocalIp(serverType, inetAddress)) {
                return true;
            }
        }
        return false;
    }

    public boolean isCrossRegionAccess(String serverType, InetSocketAddress socketAddress)
    {
        return isValidLocalIp(serverType, extractInetAddress(socketAddress));
    }

    private boolean isValidLocalIp(String serverType, InetAddress inetAddress)
    {
        if (!localIpRangeMatcher.matches(inetAddress)) {
            if (!crossRegionAllowed) {
                throw new UncheckedIOException(new IOException(format("%s %s is not in an allowed region", serverType, toAddrString(inetAddress))));
            }
            return true;
        }
        return false;
    }

    private static List<String> getRegionLocalIpAddresses(Properties properties)
    {
        return Splitter.on(',').splitToList(
                requireNonNull(properties.getProperty(REGION_LOCAL_IP_ADDRESSES_PROPERTY_NAME),
                        "Missing required property: " + REGION_LOCAL_IP_ADDRESSES_PROPERTY_NAME));
    }

    public static void addRegionLocalIpAddresses(Properties properties, List<String> regionLocalIpAddresses)
    {
        properties.setProperty(REGION_LOCAL_IP_ADDRESSES_PROPERTY_NAME, Joiner.on(",").join(regionLocalIpAddresses));
    }

    private static InetAddress extractInetAddress(InetSocketAddress socketAddress)
    {
        checkArgument(!socketAddress.isUnresolved(), "IP address should already be resolved");
        return socketAddress.getAddress();
    }

    public static void addCrossRegionAllowed(Properties properties, boolean crossRegionAllowed)
    {
        properties.setProperty(CROSS_REGION_ALLOWED_PROPERTY_NAME, Boolean.toString(crossRegionAllowed));
    }

    private static boolean isCrossRegionAllowed(Properties properties)
    {
        return Boolean.parseBoolean(getRequiredProperty(properties, CROSS_REGION_ALLOWED_PROPERTY_NAME));
    }

    private static String getRequiredProperty(Properties properties, String propertyName)
    {
        return getOptionalProperty(properties, propertyName)
                .orElseThrow(() -> new IllegalArgumentException("Missing required property: " + propertyName));
    }

    private static Optional<String> getOptionalProperty(Properties properties, String propertyName)
    {
        return Optional.ofNullable(properties.getProperty(propertyName));
    }
}
