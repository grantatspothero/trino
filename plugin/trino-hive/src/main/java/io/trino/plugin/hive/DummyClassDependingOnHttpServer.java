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

package io.trino.plugin.hive;

public final class DummyClassDependingOnHttpServer
{
    // add dummy dependency on io.airlift.http.server to please
    // deps checker.
    // Otherwise we got:
    //
    // [ERROR] Found a problem with test-scoped dependency io.airlift:http-server
    // [ERROR] Scope compile was expected by artifact io.airlift:jaxrs:241
    // [ERROR]
    // [ERROR] Dependency chain:
    // [ERROR] io.trino:trino-hive:trino-plugin:440-galaxy-1-SNAPSHOT
    // [ERROR] \- io.starburst.stargate:metastore-client:jar:stargate-14167-g7f647d4448:compile
    // [ERROR]    \- io.starburst.stargate:stargate-common:jar:stargate-14167-g7f647d4448:compile
    // [ERROR]       \- io.starburst.stargate:stargate-exceptions:jar:stargate-14167-g7f647d4448:compile
    // [ERROR]          \- io.starburst.api-builder:api-builder:jar:api-builder-20240314-dbca6a4:compile
    // [ERROR]             \- io.airlift:jaxrs:jar:241:compile
    // [ERROR]                \- io.airlift:http-server:jar:241:compile
    // [ERROR] Scope runtime was expected by artifact io.starburst.api-builder:api-builder:api-builder-20240314-dbca6a4
    // [ERROR]
    // [ERROR] Dependency chain:
    // [ERROR] io.trino:trino-hive:trino-plugin:440-galaxy-1-SNAPSHOT
    // [ERROR] \- io.starburst.stargate:metastore-client:jar:stargate-14167-g7f647d4448:compile
    // [ERROR]    \- io.starburst.stargate:stargate-common:jar:stargate-14167-g7f647d4448:compile
    // [ERROR]       \- io.starburst.stargate:stargate-exceptions:jar:stargate-14167-g7f647d4448:compile
    // [ERROR]          \- io.starburst.api-builder:api-builder:jar:api-builder-20240314-dbca6a4:compile
    // [ERROR]             \- io.airlift:http-server:jar:239:runtime
    // [ERROR] Scope compile was expected by artifact io.starburst.stargate:stargate-common:stargate-14167-g7f647d4448
    // [ERROR]
    // [ERROR] Dependency chain:
    // [ERROR] io.trino:trino-hive:trino-plugin:440-galaxy-1-SNAPSHOT
    // [ERROR] \- io.starburst.stargate:metastore-client:jar:stargate-14167-g7f647d4448:compile
    // [ERROR]    \- io.starburst.stargate:stargate-common:jar:stargate-14167-g7f647d4448:compile
    // [ERROR]       \- io.airlift:http-server:jar:240:compile
    public static final io.airlift.http.server.HttpServerInfo HTTP_SERVER_INFO = null;

    private DummyClassDependingOnHttpServer() {}
}
