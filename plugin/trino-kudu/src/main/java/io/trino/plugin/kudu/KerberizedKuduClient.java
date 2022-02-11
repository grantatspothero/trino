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
package io.trino.plugin.kudu;

import io.trino.plugin.base.authentication.CachingKerberosAuthentication;
import net.jodah.failsafe.ExecutionContext;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.Status;

import javax.security.auth.Subject;

import java.security.PrivilegedAction;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static org.apache.kudu.client.KuduClient.KuduClientBuilder;

public class KerberizedKuduClient
        extends ForwardingKuduClient
{
    private final KuduClient kuduClient;
    private final CachingKerberosAuthentication cachingKerberosAuthentication;
    private final RetryPolicy<Object> kerberosRetryPolicy = new RetryPolicy<>().withMaxAttempts(2).handleIf((Exception e) -> {
        if (e instanceof UncheckedKuduException) {
            KuduException kuduException = ((UncheckedKuduException) e).getCause();
            Status status = kuduException.getStatus();
            return status.isServiceUnavailable() && kuduException.getMessage().contains("server requires authentication, but client Kerberos credentials (TGT) have expired");
        }
        return false;
    });

    KerberizedKuduClient(KuduClientBuilder kuduClientBuilder, CachingKerberosAuthentication cachingKerberosAuthentication)
    {
        requireNonNull(kuduClientBuilder, "kuduClientBuilder is null");
        this.cachingKerberosAuthentication = requireNonNull(cachingKerberosAuthentication, "cachingKerberosAuthentication is null");
        kuduClient = Subject.doAs(cachingKerberosAuthentication.getSubject(), (PrivilegedAction<KuduClient>) (kuduClientBuilder::build));
    }

    @Override
    protected <R> R delegate(Function<KuduClient, R> function)
    {
        return Failsafe.with(kerberosRetryPolicy).get((ExecutionContext context) -> {
            if (!context.isFirstAttempt()) {
                cachingKerberosAuthentication.reauthenticateIfSoonWillBeExpired();
            }
            return function.apply(kuduClient);
        });
    }
}
