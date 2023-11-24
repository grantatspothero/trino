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
package io.trino.plugin.galaxy.catalog;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.HttpStatusListener;
import io.starburst.stargate.accesscontrol.client.GalaxyLanguageFunctionApi;
import io.starburst.stargate.accesscontrol.client.HttpGalaxyFunctionClient;
import io.starburst.stargate.http.RetryingHttpClient;

import static io.airlift.http.client.HttpClientBinder.httpClientBinder;

class GalaxyCatalogFunctionApiModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        httpClientBinder(binder).bindHttpClient("galaxy-catalog", ForGalaxyCatalog.class);
    }

    @Provides
    @Singleton
    public HttpClient httpClient(@ForGalaxyCatalog HttpClient rawHttpClient)
    {
        return new RetryingHttpClient(rawHttpClient).withHttpStatusListener(new InvalidServiceStatusListener());
    }

    @Provides
    @Singleton
    public GalaxyLanguageFunctionApi galaxyFunctionApi(HttpClient httpClient, GalaxyCatalogConfig config)
    {
        return new HttpGalaxyFunctionClient(config.getAccountUri(), httpClient);
    }

    private static class InvalidServiceStatusListener
            implements HttpStatusListener
    {
        @Override
        public void statusReceived(int statusCode)
        {
            if ((statusCode == HttpStatus.BAD_GATEWAY.code()) || (statusCode == HttpStatus.GATEWAY_TIMEOUT.code()) || (statusCode == HttpStatus.SERVICE_UNAVAILABLE.code())) {
                throw new RetryingHttpClient.RetryableException("Retryable status received: " + statusCode);
            }
        }
    }
}
