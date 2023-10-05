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

import com.datastax.oss.driver.internal.core.context.DefaultNettyOptions;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import io.netty.channel.Channel;

import static java.util.Objects.requireNonNull;

public class GalaxyDefaultNettyOptions
        extends DefaultNettyOptions
{
    private final GalaxyCassandraTunnelManager galaxyCassandraTunnelManager;

    public GalaxyDefaultNettyOptions(InternalDriverContext context, GalaxyCassandraTunnelManager galaxyCassandraTunnelManager)
    {
        super(context);
        this.galaxyCassandraTunnelManager = requireNonNull(galaxyCassandraTunnelManager, "galaxyCassandraTunnelManager is null");
    }

    @Override
    public Class<? extends Channel> channelClass()
    {
        return GalaxyNioSocketChannel.class;
    }

    @Override
    public void afterChannelInitialized(Channel channel)
    {
        GalaxyNioSocketChannel galaxyNioSocketChannel = (GalaxyNioSocketChannel) channel;
        galaxyNioSocketChannel.setGalaxyCassandraTunnelManager(galaxyCassandraTunnelManager);
    }
}
