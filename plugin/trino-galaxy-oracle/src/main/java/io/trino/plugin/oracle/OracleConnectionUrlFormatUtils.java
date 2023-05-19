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
package io.trino.plugin.oracle;

import com.google.common.net.HostAndPort;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.net.HostAndPort.fromParts;

/**
 * Utility class used to
 * <ul>
 * <li>1. parse the oracle connection string to get host and port.</li>
 * <li>2. create connection url with ssh tunnel</li>
 * </ul>
 * Supports <a href="https://docs.oracle.com/en/database/oracle/oracle-database/21/jajdb/">EZConnect</a> connection URL format
 */

public class OracleConnectionUrlFormatUtils
{
    private static final String CONNECTION_URL_REGEX = "@(?:tcp[s]?://|//)?([^:/?]+)(?::(\\d+))?";
    private static final Pattern CONNECTION_URL_REGEX_PATTERN = Pattern.compile(CONNECTION_URL_REGEX);
    private static final String DEFAULT_PORT = "1521";

    private OracleConnectionUrlFormatUtils() {}

    public static HostAndPort getHostPort(String connectionUrl)
    {
        Matcher matcher = matcher(connectionUrl);
        String host = matcher.group(1);
        int port = Integer.parseInt(matcher.group(2) == null ? DEFAULT_PORT : matcher.group(2));
        return fromParts(host, port);
    }

    private static Matcher matcher(String connectionUrl)
    {
        Matcher matcher = CONNECTION_URL_REGEX_PATTERN.matcher(connectionUrl);
        checkArgument(matcher.find(), "Invalid URL: %s".formatted(connectionUrl));
        return matcher;
    }

    public static String buildSshTunnelConnectionUrl(String connectionUrl, int sshTunnelPort)
    {
        Matcher matcher = matcher(connectionUrl);
        String endpoint = matcher.group(1);
        boolean hasPort = matcher.group(2) != null;
        if (hasPort) {
            endpoint = endpoint + ":" + matcher.group(2);
        }
        HostAndPort sshTunnelHostAndPort = fromParts("127.0.0.1", sshTunnelPort);
        return connectionUrl.replaceFirst(endpoint, sshTunnelHostAndPort.toString());
    }
}
