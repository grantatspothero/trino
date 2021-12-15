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
package io.trino.server.security.galaxy;

import com.google.common.io.Closer;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.starburst.stargate.accesscontrol.client.testing.TestingAccountClient;
import io.starburst.stargate.accesscontrol.client.testing.TestingPortalClient;

import java.io.IOException;
import java.net.URI;

public class IdeTestingAccountFactory
        implements TestingAccountFactory
{
    private final Closer closer = Closer.create();
    private final TestingPortalClient portalClient;

    public IdeTestingAccountFactory()
    {
        portalClient = new TestingPortalClient(URI.create("https://local.gate0.net:8888"), closer.register(new JettyHttpClient()));
    }

    @Override
    public TestingAccountClient createAccount()
    {
        return portalClient.createAccount();
    }

    @Override
    public void close()
            throws IOException
    {
        closer.close();
    }
}
