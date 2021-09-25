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
package io.trino.plugin.elasticsearch.client.galaxy;

import com.google.common.collect.ImmutableList;
import com.google.common.net.InternetDomainName;
import io.trino.plugin.base.galaxy.IpRangeMatcher;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.protocol.HttpContext;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import static com.google.common.net.InetAddresses.isInetAddress;
import static com.google.common.net.InetAddresses.toAddrString;
import static java.lang.String.format;
import static org.apache.http.protocol.HttpCoreContext.HTTP_TARGET_HOST;

public class RegionEnforcerInterceptor
        implements HttpRequestInterceptor
{
    private final IpRangeMatcher ipRangeMatcher;

    public RegionEnforcerInterceptor(List<String> allowedIpAddresses)
    {
        this.ipRangeMatcher = IpRangeMatcher.create(allowedIpAddresses);
    }

    @Override
    public void process(HttpRequest request, HttpContext context)
            throws IOException
    {
        HttpHost host = (HttpHost) context.getAttribute(HTTP_TARGET_HOST);
        verifyAllowedIps("Elasticsearch", host.getHostName());
    }

    private List<InetAddress> toInetAddresses(String host)
    {
        if (!InternetDomainName.isValid(host) && !isInetAddress(host)) {
            throw new IllegalArgumentException(format("Malformed hostname or IP address: %s", host));
        }
        try {
            return ImmutableList.copyOf(InetAddress.getAllByName(host));
        }
        catch (UnknownHostException e) {
            throw new IllegalArgumentException(format("No DNS records found for host: %s", host));
        }
    }

    public void verifyAllowedIps(String serverType, String host)
            throws IOException
    {
        for (InetAddress inetAddress : toInetAddresses(host)) {
            if (!ipRangeMatcher.matches(inetAddress)) {
                throw new IOException(format("%s host '%s' that resolves to IP %s is not in an allowed region", serverType, host, toAddrString(inetAddress)));
            }
        }
    }
}
