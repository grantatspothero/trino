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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.trino.spi.galaxy.CatalogConnectionType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.net.InetAddresses.toAddrString;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.plugin.base.galaxy.InetAddresses.toInetAddresses;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RegionVerifier
{
    // Use static caches to retain state because socket factories are unfortunately recreated for each socket
    private static final LoadingCache<List<String>, IpRangeMatcher> IP_RANGE_MATCHER_CACHE =
            buildNonEvictableCache(CacheBuilder.newBuilder(), CacheLoader.from(IpRangeMatcher::create));

    private final IpRangeMatcher localIpRangeMatcher;
    private final boolean crossRegionAllowed;

    public RegionVerifier(RegionVerifierProperties properties)
    {
        requireNonNull(properties, "properties is null");
        this.crossRegionAllowed = properties.crossRegionAllowed();
        this.localIpRangeMatcher = IP_RANGE_MATCHER_CACHE.getUnchecked(requireNonNull(properties.localRegionIpAddresses(), "localRegionIpAddresses is null"));
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

    public boolean isPrivateLinkAccess(List<InetAddress> addresses)
    {
        for (InetAddress inetAddress : addresses) {
            if (isPrivateLinkAccess(inetAddress)) {
                return true;
            }
        }
        return false;
    }

    public boolean isPrivateLinkAccess(InetSocketAddress socketAddress)
    {
        return isPrivateLinkAccess(extractInetAddress(socketAddress));
    }

    public boolean isPrivateLinkAccess(InetAddress inetAddress)
    {
        return inetAddress.getHostAddress().startsWith("172.16.");
    }

    public CatalogConnectionType getCatalogConnectionType(String serverType, InetSocketAddress socketAddress)
    {
        if (isPrivateLinkAccess(socketAddress)) {
            return CatalogConnectionType.PRIVATE_LINK;
        }
        if (isCrossRegionAccess(serverType, socketAddress)) {
            return CatalogConnectionType.CROSS_REGION;
        }
        return CatalogConnectionType.INTRA_REGION;
    }

    public CatalogConnectionType getCatalogConnectionType(String serverType, List<InetAddress> addresses)
    {
        if (isPrivateLinkAccess(addresses)) {
            return CatalogConnectionType.PRIVATE_LINK;
        }
        if (isCrossRegionAccess(serverType, addresses)) {
            return CatalogConnectionType.CROSS_REGION;
        }
        return CatalogConnectionType.INTRA_REGION;
    }

    private static InetAddress extractInetAddress(InetSocketAddress socketAddress)
    {
        checkArgument(!socketAddress.isUnresolved(), "IP address should already be resolved");
        return socketAddress.getAddress();
    }
}
