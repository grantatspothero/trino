/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.metadata.server;

import io.airlift.bootstrap.ApplicationConfigurationException;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.http.server.HttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.jmx.JmxModule;
import io.airlift.json.JsonModule;
import io.airlift.log.LogJmxModule;
import io.airlift.log.Logger;
import io.airlift.node.NodeModule;
import io.airlift.tracetoken.TraceTokenModule;
import io.starburst.stargate.buffer.metadata.database.DatabaseModule;
import org.weakref.jmx.guice.MBeanModule;

import static io.starburst.stargate.buffer.metadata.database.DatabaseSetup.setupDatabase;

public final class MetadataServer
{
    private static final Logger log = Logger.get(MetadataServer.class);

    private MetadataServer() {}

    public static void main(String[] args)
    {
        Bootstrap app = new Bootstrap(
                new NodeModule(),
                new HttpServerModule(),
                new JsonModule(),
                new JaxrsModule(),
                new MBeanModule(),
                new JmxModule(),
                new LogJmxModule(),
                new TraceTokenModule(),
                new DatabaseModule(),
                new MainModule());

        try {
            setupDatabase();
            app.strictConfig().initialize();

            log.info("======== SERVER STARTED ========");
        }
        catch (ApplicationConfigurationException e) {
            log.error(e.getMessage());
            System.exit(1);
        }
        catch (Throwable e) {
            log.error(e);
            System.exit(1);
        }
    }
}
