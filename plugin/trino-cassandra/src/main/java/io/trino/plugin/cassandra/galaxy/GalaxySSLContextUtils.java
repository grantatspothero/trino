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
package io.trino.plugin.cassandra.galaxy;

import io.trino.spi.TrinoException;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;

import static io.trino.plugin.cassandra.CassandraErrorCode.CASSANDRA_SSL_INITIALIZATION_FAILURE;

public final class GalaxySSLContextUtils
{
    private GalaxySSLContextUtils() {}

    public static SSLContext getInsecureSslContext()
    {
        try {
            X509TrustManager trustAllCerts = new X509TrustManager()
            {
                @Override
                public void checkClientTrusted(X509Certificate[] chain, String authType)
                {
                    throw new UnsupportedOperationException("checkClientTrusted should not be called");
                }

                @Override
                public void checkServerTrusted(X509Certificate[] chain, String authType)
                {
                    // skip validation of server certificate
                }

                @Override
                public X509Certificate[] getAcceptedIssuers()
                {
                    return new X509Certificate[0];
                }
            };

            SSLContext sslContext = SSLContext.getInstance("SSL");
            sslContext.init(null, new TrustManager[] {trustAllCerts}, new SecureRandom());

            return sslContext;
        }
        catch (GeneralSecurityException e) {
            throw new TrinoException(CASSANDRA_SSL_INITIALIZATION_FAILURE, e);
        }
    }
}
