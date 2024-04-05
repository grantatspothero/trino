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
package io.trino.server.galaxy.catalogs;

import io.starburst.stargate.id.AccountId;
import io.starburst.stargate.id.CatalogVersion;
import io.starburst.stargate.id.DeploymentId;
import io.starburst.stargate.id.TrinoPlaneId;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public enum AlertingLiveCatalogsErrorType
{
    SECRETS_DECRYPTION("decrypting catalog secrets"),
    CATALOG_INITIALIZATION("initializing a live catalog"),
    CATALOG_CONFIGURATION("fetching catalog version configuration"),
    /**/;

    private final String errorOccurredWhile;

    AlertingLiveCatalogsErrorType(String errorOccurredWhile)
    {
        this.errorOccurredWhile = requireNonNull(errorOccurredWhile, "errorOccurredWhile is null");
    }

    public String getErrorOccurredWhile()
    {
        return errorOccurredWhile;
    }

    public static String buildAlertingLiveCatalogsErrorMessage(
            AlertingLiveCatalogsErrorType errorType,
            AccountId accountId,
            CatalogVersion catalogVersion,
            Optional<DeploymentId> deploymentId,
            Optional<TrinoPlaneId> trinoPlaneId)
    {
        String liveCatalogsLogPrefix = buildLiveCatalogsLogPrefix(accountId, catalogVersion, deploymentId, trinoPlaneId);
        return "%s An unexpected live catalogs error occurred while %s".formatted(liveCatalogsLogPrefix, errorType.getErrorOccurredWhile());
    }

    private static String buildLiveCatalogsLogPrefix(
            AccountId accountId,
            CatalogVersion catalogVersion,
            Optional<DeploymentId> deploymentId,
            Optional<TrinoPlaneId> trinoPlaneId)
    {
        StringBuilder prefix = new StringBuilder("[AccountId:%s][CatalogVersion:%s]".formatted(accountId, catalogVersion));
        deploymentId.ifPresent(id -> prefix.append("[DeploymentId:%s]".formatted(id)));
        trinoPlaneId.ifPresent(id -> prefix.append("[TrinoPlaneId:%s]".formatted(id)));
        return prefix.toString();
    }
}
