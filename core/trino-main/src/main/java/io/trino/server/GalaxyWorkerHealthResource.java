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
package io.trino.server;

import io.airlift.discovery.client.ForDiscoveryClient;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceDescriptorsRepresentation;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.trino.client.GalaxyWorkerHealthStatus;
import io.trino.server.security.ResourceSecurity;
import io.trino.server.security.ResourceSecurity.AccessType;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import java.net.URI;
import java.util.function.Supplier;

import static com.google.common.base.Verify.verify;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;

@Path("/v1/galaxy/health")
public class GalaxyWorkerHealthResource
{
    private static final Logger log = Logger.get(GalaxyWorkerHealthResource.class);

    private final NodeInfo nodeInfo;
    private final Supplier<URI> discoveryServiceUri;
    private final HttpClient httpClient;
    private final JsonCodec<ServiceDescriptorsRepresentation> serviceDescriptorsCodec;

    @Inject
    public GalaxyWorkerHealthResource(
            NodeInfo nodeInfo,
            @ForDiscoveryClient Supplier<URI> discoveryServiceUri,
            @ForDiscoveryClient HttpClient httpClient,
            JsonCodec<ServiceDescriptorsRepresentation> serviceDescriptorsCodec)
    {
        this.nodeInfo = requireNonNull(nodeInfo, "nodeInfo is null");
        this.discoveryServiceUri = requireNonNull(discoveryServiceUri, "discoveryServiceUri is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.serviceDescriptorsCodec = requireNonNull(serviceDescriptorsCodec, "serviceDescriptorsCodec is null");
    }

    @ResourceSecurity(AccessType.PUBLIC)
    @GET
    @Produces(APPLICATION_JSON)
    public Response checkWorkerHealth()
    {
        GalaxyWorkerHealthStatus workerHealthStatus = new GalaxyWorkerHealthStatus(
                nodeInfo.getNodeId(),
                isRegisteredInDiscovery());

        if (workerHealthStatus.isHealthy()) {
            return Response.ok(workerHealthStatus).build();
        }
        return Response.status(SERVICE_UNAVAILABLE)
                .entity(workerHealthStatus)
                .build();
    }

    private boolean isRegisteredInDiscovery()
    {
        URI discoveryUri = discoveryServiceUri.get();
        verify(discoveryUri != null, "discovery.uri expected to be configured");

        URI uri = uriBuilderFrom(discoveryUri)
                .appendPath("/v1/service/trino")
                .build();
        Request request = prepareGet()
                .setUri(uri)
                .setHeader("User-Agent", nodeInfo.getNodeId())
                .build();

        try {
            // Call discovery service directly to avoid any caching
            ServiceDescriptorsRepresentation serviceDescriptors = httpClient.execute(request, createJsonResponseHandler(serviceDescriptorsCodec));

            return serviceDescriptors.getServiceDescriptors().stream()
                    .map(ServiceDescriptor::getNodeId)
                    .anyMatch(serviceNodeId -> nodeInfo.getNodeId().equals(serviceNodeId));
        }
        catch (RuntimeException e) {
            log.error(e, "Failed to contact discovery service for Galaxy health check");
            return false;
        }
    }
}
