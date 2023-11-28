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
package io.trino.plugin.base.galaxy;

import com.google.common.annotations.VisibleForTesting;
import io.starburst.stargate.id.AccountId;
import io.starburst.stargate.id.RoleId;
import io.starburst.stargate.id.UserId;
import io.starburst.stargate.identity.DispatchSession;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.Identity;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public final class GalaxyIdentity
{
    private GalaxyIdentity() {}

    @VisibleForTesting
    public static Identity createIdentity(String username, AccountId accountId, UserId userId, RoleId roleId, String token)
    {
        return Identity.forUser(username)
                .withExtraCredentials(Map.of(
                        "accountId", accountId.toString(),
                        "userId", userId.toString(),
                        "roleId", roleId.toString(),
                        "GalaxyTokenCredential", token))
                .build();
    }

    public static DispatchSession toDispatchSession(ConnectorIdentity identity)
    {
        return new DispatchSession(
                new AccountId(credential(identity, "accountId")),
                new UserId(credential(identity, "userId")),
                new RoleId(credential(identity, "roleId")),
                credential(identity, "GalaxyTokenCredential"));
    }

    private static String credential(ConnectorIdentity identity, String name)
    {
        String value = identity.getExtraCredentials().get(name);
        checkArgument(value != null, "extra credential not set: %s", name);
        return value;
    }
}
