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
package io.trino.tests.exchange.buffer;

import static com.google.common.base.Verify.verify;

public class TestStargateBufferExchangeDistributedEngineOnlyQueries
        extends AbstractBufferExchangeDistributedEngineOnlyQueries
{
    @Override
    protected String getBufferServiceVersion()
    {
        String stargateBufferExchangeVersion = System.getenv("STARGATE_BUFFER_EXCHANGE_VERSION");
        verify(stargateBufferExchangeVersion != null && !stargateBufferExchangeVersion.isBlank(), "STARGATE_BUFFER_EXCHANGE_VERSION not set");
        return stargateBufferExchangeVersion;
    }
}
