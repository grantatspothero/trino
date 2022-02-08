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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.validation.FileExists;

import javax.validation.constraints.NotNull;

import java.io.File;
import java.util.Optional;

public class KuduKerberosConfig
{
    private String clientPrincipal;
    private String clientKeytab;
    private File config;
    // The kudu client defaults to using "kudu" if this is undefined
    private Optional<String> kuduPrincipalPrimary = Optional.empty();

    @NotNull
    public String getClientPrincipal()
    {
        return clientPrincipal;
    }

    @Config("kerberos.client.principal")
    @ConfigDescription("Client principal")
    public KuduKerberosConfig setClientPrincipal(String clientPrincipal)
    {
        this.clientPrincipal = clientPrincipal;
        return this;
    }

    @NotNull
    @FileExists
    public String getClientKeytab()
    {
        return clientKeytab;
    }

    @Config("kerberos.client.keytab")
    @ConfigDescription("Client keytab location")
    public KuduKerberosConfig setClientKeytab(String clientKeytab)
    {
        this.clientKeytab = clientKeytab;
        return this;
    }

    @NotNull
    @FileExists
    public File getConfig()
    {
        return config;
    }

    @Config("kerberos.config")
    @ConfigDescription("Kerberos service configuration file")
    public KuduKerberosConfig setConfig(File config)
    {
        this.config = config;
        return this;
    }

    public Optional<String> getKuduPrincipalPrimary()
    {
        return kuduPrincipalPrimary;
    }

    @Config("kerberos.kudu.principal.primary")
    @ConfigDescription("The 'primary' portion of the kudu service principal name")
    public KuduKerberosConfig setKuduPrincipalPrimary(String kuduPrincipalPrimary)
    {
        this.kuduPrincipalPrimary = Optional.ofNullable(kuduPrincipalPrimary);
        return this;
    }
}
