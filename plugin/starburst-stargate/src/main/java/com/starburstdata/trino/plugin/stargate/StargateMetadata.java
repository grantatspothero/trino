/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate;

import io.trino.plugin.jdbc.DefaultJdbcMetadata;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcQueryEventListener;
import io.trino.spi.connector.ConnectorSession;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class StargateMetadata
        extends DefaultJdbcMetadata
{
    private final StargateCatalogIdentityFactory catalogIdentityFactory;

    public StargateMetadata(StargateCatalogIdentityFactory catalogIdentityFactory, JdbcClient jdbcClient, Set<JdbcQueryEventListener> jdbcQueryEventListeners)
    {
        // Disable precalculate statistics for pushdown because remote cluster is very good at estimates, should not be inferior to ours.
        super(jdbcClient, false, jdbcQueryEventListeners);
        this.catalogIdentityFactory = requireNonNull(catalogIdentityFactory, "catalogIdentityFactory is null");
    }

    @Override
    public String getCatalogIdentity(ConnectorSession session)
    {
        return catalogIdentityFactory.getCatalogIdentity(session);
    }
}
