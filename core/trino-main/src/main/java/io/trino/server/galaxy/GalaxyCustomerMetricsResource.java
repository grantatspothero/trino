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
package io.trino.server.galaxy;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.node.NodeInfo;
import io.airlift.openmetrics.MetricsConfig;
import io.airlift.openmetrics.MetricsResource;
import io.trino.server.security.ResourceSecurity;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import org.weakref.jmx.MBeanExporter;

import javax.management.MBeanServer;

import java.util.List;

import static io.trino.server.security.ResourceSecurity.AccessType.AUTHENTICATED_USER;

@Path("/galaxy/metrics")
public class GalaxyCustomerMetricsResource
        extends MetricsResource
{
    private static final String OPENMETRICS_CONTENT_TYPE = "application/openmetrics-text; version=1.0.0; charset=utf-8";

    @Inject
    public GalaxyCustomerMetricsResource(
            MBeanServer mbeanServer,
            @ForCustomerMetrics MBeanExporter mbeanExporter,
            MetricsConfig metricsConfig,
            NodeInfo nodeInfo)
    {
        super(mbeanServer, mbeanExporter, metricsConfig, nodeInfo);
    }

    @Override
    @GET
    @Produces(OPENMETRICS_CONTENT_TYPE)
    @ResourceSecurity(AUTHENTICATED_USER)
    public String getMetrics(List<String> ignored)
    {
        // Ignoring the filter parameter to prevent customers from pulling metrics from jmxutils directly
        return super.getMetrics(ImmutableList.of());
    }
}
