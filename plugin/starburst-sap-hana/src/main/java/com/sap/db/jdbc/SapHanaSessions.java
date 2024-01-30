/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.sap.db.jdbc;

import java.net.Socket;

// Shim class to expose a protected method
public class SapHanaSessions
{
    private SapHanaSessions() {}

    public static void setSocket(Session session, Socket socket)
    {
        session._setSocketOptions(socket);
    }
}
