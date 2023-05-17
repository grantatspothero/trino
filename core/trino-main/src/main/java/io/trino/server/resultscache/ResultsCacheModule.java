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

package io.trino.server.resultscache;

import com.google.inject.Binder;
import com.google.inject.Module;
import io.trino.SystemSessionPropertiesProvider;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;

public class ResultsCacheModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        newSetBinder(binder, SystemSessionPropertiesProvider.class).addBinding().to(ResultsCacheSessionProperties.class);
        configBinder(binder).bindConfig(ResultsCacheConfig.class);
        httpClientBinder(binder).bindHttpClient("client", ForResultsCache.class);
        binder.bind(ResultsCacheManager.class).in(SINGLETON);
        binder.bind(ResultsCacheAnalyzerFactory.class).in(SINGLETON);
    }
}
