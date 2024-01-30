/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.saphana;

import com.sap.db.jdbc.ConnectionSapDB;
import com.sap.db.jdbc.SapHanaSessions;
import com.sap.db.jdbc.Session;
import io.trino.plugin.base.galaxy.GalaxySqlSocketFactory;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.credential.CredentialPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkState;
import static com.sap.db.jdbc.ConnectionProperty.NON_BLOCKING_IO;
import static java.util.Objects.requireNonNull;

public class GalaxySapHanaConnectionFactory
        implements ConnectionFactory
{
    private final Driver driver;
    private final String connectionUrl;
    private final Properties socketFactoryProperties;
    private final CredentialPropertiesProvider<String, String> credentialPropertiesProvider;

    public GalaxySapHanaConnectionFactory(
            Driver driver,
            String connectionUrl,
            Properties socketFactoryProperties,
            CredentialPropertiesProvider<String, String> credentialPropertiesProvider)
    {
        this.driver = requireNonNull(driver, "driver is null");
        this.connectionUrl = requireNonNull(connectionUrl, "connectionUrl is null");
        this.socketFactoryProperties = requireNonNull(socketFactoryProperties, "socketFactoryProperties is null");
        this.credentialPropertiesProvider = requireNonNull(credentialPropertiesProvider, "credentialPropertiesProvider is null");
    }

    @Override
    public Connection openConnection(ConnectorSession session)
            throws SQLException
    {
        Properties properties = getCredentialProperties(session.getIdentity());
        properties.put(NON_BLOCKING_IO.getName(), "false");
        ConnectionSapDB connection = (ConnectionSapDB) driver.connect(connectionUrl, properties);
        checkState(connection != null, "Driver returned null connection, make sure the connection URL '%s' is valid for the driver %s", connectionUrl, driver);

        Session primarySession = connection.getSessionPool().getPrimarySession();
        SapHanaSessions.setSocket(primarySession, new GalaxySqlSocketFactory(socketFactoryProperties).createSocket());
        connection.getSessionPool().setPrimarySession(primarySession);

        return connection;
    }

    private Properties getCredentialProperties(ConnectorIdentity identity)
    {
        Properties properties = new Properties();
        properties.putAll(credentialPropertiesProvider.getCredentialProperties(identity));
        return properties;
    }
}
