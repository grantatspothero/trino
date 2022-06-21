/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.metadata.database;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

public class DatabaseConfig
{
    private String url;
    private String user;
    private String password;

    @NotEmpty
    public String getUrl()
    {
        return url;
    }

    @Config("db.url")
    public DatabaseConfig setUrl(String url)
    {
        this.url = url;
        return this;
    }

    @NotEmpty
    public String getUser()
    {
        return user;
    }

    @Config("db.user")
    public DatabaseConfig setUser(String user)
    {
        this.user = user;
        return this;
    }

    @NotNull
    public String getPassword()
    {
        return password;
    }

    @Config("db.password")
    @ConfigSecuritySensitive
    public DatabaseConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }
}
