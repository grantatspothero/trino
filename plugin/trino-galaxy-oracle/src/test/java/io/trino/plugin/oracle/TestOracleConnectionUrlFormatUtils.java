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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static com.google.common.net.HostAndPort.fromParts;
import static io.trino.plugin.oracle.OracleConnectionUrlFormatUtils.buildSshTunnelConnectionUrl;
import static io.trino.plugin.oracle.OracleConnectionUrlFormatUtils.getHostPort;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOracleConnectionUrlFormatUtils
{
    @ParameterizedTest
    @MethodSource("hostPortProvider")
    public void testHostAndPort(String connectionUrl, HostAndPort expectedHostAndPort)
    {
        assertThat(getHostPort(connectionUrl)).isEqualTo(expectedHostAndPort);
    }

    @ParameterizedTest
    @MethodSource("sshTunnelUrlProvider")
    public void testSshTunnelConnectionUrl(String connectionUrl, int sshTunnelPort, String expectedSshTunnelConnectionUrl)
    {
        assertThat(buildSshTunnelConnectionUrl(connectionUrl, sshTunnelPort)).isEqualTo(expectedSshTunnelConnectionUrl);
    }

    public static Object[][] hostPortProvider()
    {
        return new Object[][] {
                {"jdbc:oracle:thin:@mydbhost:3233", fromParts("mydbhost", 3233)},
                {"jdbc:oracle:thin:@mydbhost", fromParts("mydbhost", 1521)},
                {"jdbc:oracle:thin:@mydbhost:3233/mydbservice", fromParts("mydbhost", 3233)},
                {"jdbc:oracle:thin:@mydbhost/mydbservice", fromParts("mydbhost", 1521)},
                {"jdbc:oracle:thin:@//mydbhost:3233/mydbservice", fromParts("mydbhost", 3233)},
                {"jdbc:oracle:thin:@//mydbhost/mydbservice", fromParts("mydbhost", 1521)},
                {"jdbc:oracle:thin:@tcp://mydbhost:3233/mydbservice", fromParts("mydbhost", 3233)},
                {"jdbc:oracle:thin:@tcp://mydbhost/mydbservice", fromParts("mydbhost", 1521)},
                {"jdbc:oracle:thin:@tcp://mydbhost:5521/mydbservice:dedicated", fromParts("mydbhost", 5521)},
                {"jdbc:oracle:thin:@tcp://mydbhost/mydbservice:dedicated", fromParts("mydbhost", 1521)},
                {"jdbc:oracle:thin:@tcps://mydbhost1:5521/mydbservice?wallet_location=/work/wallet&ssl_server_cert_dn=\"Server DN\"", fromParts("mydbhost1", 5521)},
                {"jdbc:oracle:thin:@tcps://mydbhost1/mydbservice?wallet_location=/work/wallet&ssl_server_cert_dn=\"Server DN\"", fromParts("mydbhost1", 1521)},
        };
    }

    public static Object[][] sshTunnelUrlProvider()
    {
        return new Object[][] {
                {"jdbc:oracle:thin:@mydbhost:3233", 54564, "jdbc:oracle:thin:@127.0.0.1:54564"},
                {"jdbc:oracle:thin:@mydbhost", 54564, "jdbc:oracle:thin:@127.0.0.1:54564"},
                {"jdbc:oracle:thin:@mydbhost:3233/mydbservice", 54564, "jdbc:oracle:thin:@127.0.0.1:54564/mydbservice"},
                {"jdbc:oracle:thin:@mydbhost/mydbservice", 54564, "jdbc:oracle:thin:@127.0.0.1:54564/mydbservice"},
                {"jdbc:oracle:thin:@//mydbhost:3233/mydbservice", 54564, "jdbc:oracle:thin:@//127.0.0.1:54564/mydbservice"},
                {"jdbc:oracle:thin:@//mydbhost/mydbservice", 54564, "jdbc:oracle:thin:@//127.0.0.1:54564/mydbservice"},
                {"jdbc:oracle:thin:@tcp://mydbhost:3233/mydbservice", 54564, "jdbc:oracle:thin:@tcp://127.0.0.1:54564/mydbservice"},
                {"jdbc:oracle:thin:@tcp://mydbhost/mydbservice", 54564, "jdbc:oracle:thin:@tcp://127.0.0.1:54564/mydbservice"},
                {"jdbc:oracle:thin:@tcp://mydbhost:5521/mydbservice:dedicated", 54564, "jdbc:oracle:thin:@tcp://127.0.0.1:54564/mydbservice:dedicated"},
                {"jdbc:oracle:thin:@tcp://mydbhost/mydbservice:dedicated", 54564, "jdbc:oracle:thin:@tcp://127.0.0.1:54564/mydbservice:dedicated"},
                {"jdbc:oracle:thin:@tcps://mydbhost1:5521/mydbservice?wallet_location=/work/wallet&ssl_server_cert_dn=\"Server DN\"", 54564,
                        "jdbc:oracle:thin:@tcps://127.0.0.1:54564/mydbservice?wallet_location=/work/wallet&ssl_server_cert_dn=\"Server DN\""},
                {"jdbc:oracle:thin:@tcps://mydbhost1/mydbservice?wallet_location=/work/wallet&ssl_server_cert_dn=\"Server DN\"", 54564,
                        "jdbc:oracle:thin:@tcps://127.0.0.1:54564/mydbservice?wallet_location=/work/wallet&ssl_server_cert_dn=\"Server DN\""},
        };
    }
}
