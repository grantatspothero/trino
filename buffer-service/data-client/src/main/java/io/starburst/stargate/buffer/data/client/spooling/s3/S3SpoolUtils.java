/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.client.spooling.s3;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;

import javax.annotation.Nullable;

import java.net.URI;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.nullToEmpty;
import static io.starburst.stargate.buffer.data.client.spooling.SpoolUtils.PATH_SEPARATOR;

public final class S3SpoolUtils
{
    private S3SpoolUtils() {}

    public static AwsCredentialsProvider createAwsCredentialsProvider(@Nullable String accessKey, @Nullable String secretKey)
    {
        return createAwsCredentialsProvider(Optional.ofNullable(accessKey), Optional.ofNullable(secretKey));
    }

    public static AwsCredentialsProvider createAwsCredentialsProvider(Optional<String> accessKey, Optional<String> secretKey)
    {
        if (accessKey.isPresent() && secretKey.isPresent()) {
            return StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey.get(), secretKey.get()));
        }
        if (accessKey.isEmpty() && secretKey.isEmpty()) {
            return WebIdentityTokenFileCredentialsProvider.create();
        }
        throw new IllegalArgumentException("AWS access key and secret key should be either both set or both not set");
    }

    /**
     * Helper function used to work around the fact that if you use an S3 bucket with an '_' that java.net.URI
     * behaves differently and sets the host value to null whereas S3 buckets without '_' have a properly
     * set host field. '_' is only allowed in S3 bucket names in us-east-1.
     *
     * @param uri The URI from which to extract a host value.
     * @return The host value where uri.getAuthority() is used when uri.getHost() returns null as long as no UserInfo is present.
     * @throws IllegalArgumentException If the bucket cannot be determined from the URI.
     */
    public static String getBucketName(URI uri)
    {
        if (uri.getHost() != null) {
            return uri.getHost();
        }

        if (uri.getUserInfo() == null) {
            return uri.getAuthority();
        }

        throw new IllegalArgumentException("Unable to determine S3 bucket from URI.");
    }

    public static String keyFromUri(URI uri)
    {
        checkArgument(uri.isAbsolute(), "Uri is not absolute: %s", uri);
        String key = nullToEmpty(uri.getPath());
        if (key.startsWith(PATH_SEPARATOR)) {
            key = key.substring(PATH_SEPARATOR.length());
        }
        if (key.endsWith(PATH_SEPARATOR)) {
            key = key.substring(0, key.length() - PATH_SEPARATOR.length());
        }
        return key;
    }
}
