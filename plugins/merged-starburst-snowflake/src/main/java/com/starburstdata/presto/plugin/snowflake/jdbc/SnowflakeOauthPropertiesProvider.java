/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake.jdbc;

import com.google.common.collect.ImmutableMap;
import com.starburstdata.presto.plugin.snowflake.auth.OauthCredential;
import com.starburstdata.presto.plugin.snowflake.auth.SnowflakeOauthService;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.credential.CredentialPropertiesProvider;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class SnowflakeOauthPropertiesProvider
        implements CredentialPropertiesProvider
{
    private final SnowflakeOauthService snowflakeOauthService;

    public SnowflakeOauthPropertiesProvider(SnowflakeOauthService snowflakeOauthService)
    {
        this.snowflakeOauthService = requireNonNull(snowflakeOauthService, "snowflakeOauthService is null");
    }

    @Override
    public Map<String, String> getCredentialProperties(JdbcIdentity identity)
    {
        OauthCredential cred = snowflakeOauthService.getCredential(identity);

        return ImmutableMap.<String, String>builder()
                .put("authenticator", "oauth")
                .put("token", cred.getAccessToken())
                .build();
    }
}