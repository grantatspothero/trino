/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.prestoconnector;

import com.google.common.collect.ImmutableMap;
import com.starburstdata.presto.plugin.jdbc.authtolocal.AuthToLocal;
import io.trino.plugin.jdbc.JdbcIdentity;
import io.trino.plugin.jdbc.credential.CredentialPropertiesProvider;
import io.trino.plugin.jdbc.credential.CredentialProvider;

import java.util.Map;
import java.util.Optional;

public class PrestoConnectorImpersonatingCredentialPropertiesProvider
        implements CredentialPropertiesProvider
{
    private final CredentialProvider credentialProvider;
    private final AuthToLocal authToLocal;

    public PrestoConnectorImpersonatingCredentialPropertiesProvider(CredentialProvider credentialProvider, AuthToLocal authToLocal)
    {
        this.credentialProvider = credentialProvider;
        this.authToLocal = authToLocal;
    }

    @Override
    public Map<String, String> getCredentialProperties(JdbcIdentity identity)
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        credentialProvider.getConnectionUser(Optional.of(identity)).ifPresent((user) -> {
            properties.put("user", user);
        });
        credentialProvider.getConnectionPassword(Optional.of(identity)).ifPresent((password) -> {
            properties.put("password", password);
        });

        properties.put("sessionUser", authToLocal.translate(identity));
        return properties.build();
    }
}
