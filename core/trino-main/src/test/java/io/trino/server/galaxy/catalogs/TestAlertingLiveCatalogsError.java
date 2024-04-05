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
import io.starburst.stargate.id.CatalogId;
import io.starburst.stargate.id.CatalogVersion;
import io.starburst.stargate.id.DeploymentId;
import io.starburst.stargate.id.TrinoPlaneId;
import io.starburst.stargate.id.Version;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Optional;

import static io.trino.server.galaxy.catalogs.AlertingLiveCatalogsErrorType.buildAlertingLiveCatalogsErrorMessage;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAlertingLiveCatalogsError
{
    private static final AccountId ACCOUNT_ID = new AccountId("a-12345678");
    private static final CatalogVersion CATALOG_VERSION = new CatalogVersion(new CatalogId("c-1234567890"), Version.INITIAL_VERSION);
    private static final Optional<DeploymentId> DEPLOYMENT_ID = Optional.of(new DeploymentId("dep-123456789012"));
    private static final Optional<TrinoPlaneId> TRINO_PLANE_ID = Optional.of(new TrinoPlaneId("aws-us-east1-1234"));

    @ParameterizedTest
    @EnumSource(AlertingLiveCatalogsErrorType.class)
    public void testAlertingLiveCatalogsErrorMessage(AlertingLiveCatalogsErrorType errorType)
    {
        String expected = "[AccountId:%s][CatalogVersion:%s][DeploymentId:%s][TrinoPlaneId:%s] An unexpected live catalogs error occurred while %s".formatted(
                ACCOUNT_ID, CATALOG_VERSION, DEPLOYMENT_ID.orElseThrow(), TRINO_PLANE_ID.orElseThrow(), errorType.getErrorOccurredWhile());
        String actual = buildAlertingLiveCatalogsErrorMessage(errorType, ACCOUNT_ID, CATALOG_VERSION, DEPLOYMENT_ID, TRINO_PLANE_ID);
        assertThat(actual)
                .overridingErrorMessage(() -> getOverrideErrorMessage(expected, actual))
                .isEqualTo(expected);
    }

    private static String getOverrideErrorMessage(String expected, String actual)
    {
        return """
                Alerting live catalogs error message changed!
                Changes to the alerting live catalogs error message could result in Datadog monitors failing to detect issues with live catalogs in Galaxy production environments. Don't change this error message without first confirming that all live catalogs alerts will remain functional.
                expected: <[%s]>
                actual: <[%s]>""".formatted(expected, actual);
    }
}
