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
package io.trino.plugin.redshift;

import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.spi.security.ConnectorIdentity;

import javax.inject.Inject;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class RedshiftAwsCredentialProvider
        implements CredentialProvider
{
    private final String connectionUser;

    @Inject
    public RedshiftAwsCredentialProvider(RedshiftAwsCredentialsConfig awsConfig)
    {
        requireNonNull(awsConfig, "awsConfig is null");
        this.connectionUser = requireNonNull(awsConfig.getConnectionUser(), "connectionUser is null");
    }

    @Override
    public Optional<String> getConnectionUser(Optional<ConnectorIdentity> jdbcIdentity)
    {
        return Optional.of(connectionUser);
    }

    @Override
    public Optional<String> getConnectionPassword(Optional<ConnectorIdentity> jdbcIdentity)
    {
        return Optional.empty();
    }
}
