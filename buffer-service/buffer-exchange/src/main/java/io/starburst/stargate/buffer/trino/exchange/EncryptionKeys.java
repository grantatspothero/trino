/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.trino.exchange;

import io.trino.spi.TrinoException;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import java.security.NoSuchAlgorithmException;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public final class EncryptionKeys
{
    private static final String ENCRYPTION_ALGORITHM = "AES";
    private static final int KEY_BITS = 256;

    private EncryptionKeys() {}

    public static SecretKey generateNewEncryptionKey()
    {
        try {
            KeyGenerator generator = KeyGenerator.getInstance(ENCRYPTION_ALGORITHM);
            generator.init(KEY_BITS);
            return generator.generateKey();
        }
        catch (NoSuchAlgorithmException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Could not generate secret key", e);
        }
    }

    public static SecretKey decodeEncryptionKey(byte[] keyBytes)
    {
        return new SecretKeySpec(keyBytes, 0, keyBytes.length, ENCRYPTION_ALGORITHM);
    }

    public static Cipher getCipher()
    {
        try {
            return Cipher.getInstance(ENCRYPTION_ALGORITHM);
        }
        catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Could not create cipher", e);
        }
    }
}
