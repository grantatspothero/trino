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
package io.trino.hdfs.galaxy;

import com.google.cloud.hadoop.repackaged.gcs.com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.cloud.hadoop.repackaged.gcs.com.google.api.client.http.HttpRequestInitializer;
import com.google.cloud.hadoop.repackaged.gcs.com.google.api.services.storage.Storage;
import com.google.cloud.hadoop.repackaged.gcs.com.google.api.services.storage.model.Bucket;
import com.google.cloud.hadoop.repackaged.gcs.com.google.cloud.hadoop.util.RetryHttpInitializer;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.StatusResponseHandler.StatusResponse;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.trino.plugin.base.galaxy.IpRangeMatcher;
import org.apache.hadoop.conf.Configuration;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_CONFIG_PREFIX;
import static com.google.cloud.hadoop.repackaged.gcs.com.google.cloud.hadoop.util.HadoopCredentialConfiguration.SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.http.client.Request.Builder.prepareHead;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class RegionEnforcement
{
    private static final String ENFORCE_REGION = "starburst.galaxy.enforce-region";
    private static final String S3_ALLOWED_REGION = "starburst.galaxy.s3.allowed-region";
    private static final String GCS_ALLOWED_REGION = "starburst.galaxy.gcs.allowed-region";
    private static final String AZURE_ALLOWED_IP_ADDRESSES = "starburst.galaxy.azure.allowed-ip-addresses";

    private static final Set<String> S3_SCHEMES = ImmutableSet.of("s3", "s3a", "s3n");
    private static final Set<String> GCS_SCHEMES = ImmutableSet.of("gs");
    private static final Set<String> AZURE_SCHEMES = ImmutableSet.of("wasb", "wasbs", "abfs", "abfss", "adl");

    private RegionEnforcement() {}

    public static void addEnforceRegion(Configuration configuration, boolean enabled)
    {
        configuration.setBoolean(ENFORCE_REGION, enabled);
    }

    private static boolean isEnforceRegion(Configuration configuration)
            throws IOException
    {
        return "true".equals(getRequiredProperty(configuration, ENFORCE_REGION));
    }

    public static void addS3AllowedRegion(Configuration configuration, String s3AllowedRegion)
    {
        configuration.set(S3_ALLOWED_REGION, s3AllowedRegion);
    }

    private static Optional<String> getS3AllowedRegion(Configuration configuration)
    {
        return getOptionalProperty(configuration, S3_ALLOWED_REGION);
    }

    public static void addGcsAllowedRegion(Configuration configuration, String gcsAllowedRegion)
    {
        configuration.set(GCS_ALLOWED_REGION, gcsAllowedRegion);
    }

    private static Optional<String> getGcsAllowedRegion(Configuration configuration)
    {
        return getOptionalProperty(configuration, GCS_ALLOWED_REGION);
    }

    public static void addAzureAllowedIpAddresses(Configuration configuration, List<String> allowedIpAddresses)
    {
        configuration.set(AZURE_ALLOWED_IP_ADDRESSES, Joiner.on(",").join(allowedIpAddresses));
    }

    private static Optional<List<String>> getAzureAllowedIpAddresses(Configuration configuration)
    {
        return getOptionalProperty(configuration, AZURE_ALLOWED_IP_ADDRESSES).map(value -> Splitter.on(',').splitToList(value));
    }

    public static void enforceRegion(URI uri, Configuration configuration)
            throws IOException
    {
        if (!isEnforceRegion(configuration)) {
            return;
        }

        if (S3_SCHEMES.contains(uri.getScheme())) {
            enforceS3Region(uri, configuration);
        }
        else if (GCS_SCHEMES.contains(uri.getScheme())) {
            enforceGcsRegion(uri, configuration);
        }
        else if (AZURE_SCHEMES.contains(uri.getScheme())) {
            enforceAzureRegion(uri, configuration);
        }
        else {
            throw new IOException("File system scheme is not allowed by region enforcement: " + uri.getScheme());
        }
    }

    private static void enforceS3Region(URI uri, Configuration configuration)
            throws IOException
    {
        String bucketName = extractBucketName(uri);
        String allowedRegion = getS3AllowedRegion(configuration)
                .orElseThrow(() -> new IOException("S3 storage is not allowed"));

        String bucketLocation = lookupS3BucketRegion(bucketName);
        if (!bucketLocation.equalsIgnoreCase(allowedRegion)) {
            throw new IOException(format("S3 bucket '%s' is in region %s, but only region %s is allowed", bucketName, bucketLocation, allowedRegion));
        }
    }

    private static String lookupS3BucketRegion(String s3Bucket)
            throws IOException
    {
        checkArgument(!s3Bucket.startsWith("s3://"), "Bucket name should not include 's3://' protocol prefix: %s", s3Bucket);
        try (HttpClient httpClient = new JettyHttpClient()) {
            StatusResponse response = httpClient.execute(
                    prepareHead()
                            .setUri(URI.create("https://s3.amazonaws.com/%s".formatted(s3Bucket)))
                            .setFollowRedirects(false)
                            .build(),
                    createStatusResponseHandler());
            if (response.getStatusCode() == HttpStatus.NOT_FOUND.code()) {
                throw new IOException("S3 bucket '%s' does not exist".formatted(s3Bucket));
            }
            return response.getHeader("x-amz-bucket-region");
        }
        catch (IllegalArgumentException e) {
            throw new IOException("S3 bucket name malformed: %s".formatted(s3Bucket));
        }
    }

    private static void enforceGcsRegion(URI uri, Configuration configuration)
            throws IOException
    {
        String allowedRegion = getGcsAllowedRegion(configuration)
                .orElseThrow(() -> new IOException("Google Cloud Storage is not allowed"));

        String keyFile = configuration.get(GCS_CONFIG_PREFIX + SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX.getKey());
        if (keyFile == null) {
            throw new IOException("Google Cloud Storage JSON key file is not specified");
        }
        String gcsJsonKey = Files.readString(Paths.get(keyFile));
        // Use getRawAuthority instead of getHost because valid GCS bucket names can contain underscores (not allowed in host names)
        String bucketName = uri.getRawAuthority();
        Set<String> gcsRegions = getGcsRegions(gcsJsonKey, bucketName).stream()
                .map(regionName -> regionName.toLowerCase(Locale.ROOT))
                .collect(toImmutableSet());
        if (!gcsRegions.contains(allowedRegion.toLowerCase(Locale.ROOT))) {
            throw new IOException(format("Google Cloud Storage bucket %s is in regions %s, but only region %s is allowed", bucketName, gcsRegions, allowedRegion));
        }
    }

    private static Set<String> getGcsRegions(String gcsJsonKey, String bucketName)
            throws IOException
    {
        GoogleCredential credential = GoogleCredential.fromStream(new ByteArrayInputStream(gcsJsonKey.getBytes(UTF_8)))
                .createScoped(ImmutableList.of("https://www.googleapis.com/auth/devstorage.full_control"));

        HttpRequestInitializer initializer = new RetryHttpInitializer(credential, "Galaxy");
        Storage storage = new Storage.Builder(credential.getTransport(), credential.getJsonFactory(), initializer)
                .setApplicationName("Galaxy")
                .build();

        Bucket bucket = storage.buckets().get(bucketName).execute();

        switch (bucket.getLocationType()) {
            case "region":
                return ImmutableSet.of(bucket.getLocation());
            case "dual-region":
                return mapGcsDualRegions(bucket.getLocation());
            case "multi-region":
                throw new IOException("GCS multi-region buckets not supported");
            default:
                throw new IOException(format("GCS bucket location type '%s' not supported", bucket.getLocationType()));
        }
    }

    private static Set<String> mapGcsDualRegions(String dualRegionName)
            throws IOException
    {
        // Mapping data populated from https://cloud.google.com/storage/docs/locations
        switch (dualRegionName) {
            case "ASIA1":
                return ImmutableSet.of("asia-northeast1", "asia-northeast2");
            case "EUR4":
                return ImmutableSet.of("europe-north1", "europe-west4");
            case "NAM4":
                return ImmutableSet.of("us-central1", "us-east1");
            default:
                throw new IOException(format("GCS dual-region bucket location %s not supported", dualRegionName));
        }
    }

    private static void enforceAzureRegion(URI uri, Configuration configuration)
            throws IOException
    {
        List<String> allowedIpAddresses = getAzureAllowedIpAddresses(configuration)
                .orElseThrow(() -> new IOException("Azure storage is not allowed"));

        // TODO: cache this
        IpRangeMatcher ipMatcher = IpRangeMatcher.create(allowedIpAddresses);

        String storageAccount = uri.getHost();
        InetAddress address = InetAddress.getByName(storageAccount);
        if (!ipMatcher.matches(address)) {
            throw new IOException(format("Azure storage account %s is not in an allowed region", storageAccount));
        }
    }

    private static String getRequiredProperty(Configuration configuration, String propertyName)
            throws IOException
    {
        String value = configuration.get(propertyName);
        if (value == null) {
            // TODO: this can only be enabled once all tests have been updated
            // throw new IOException("File system configuration does not have the required property set: " + propertyName);
        }
        return value;
    }

    private static Optional<String> getOptionalProperty(Configuration config, String propertyName)
    {
        return Optional.ofNullable(config.get(propertyName));
    }

    /**
     * COPIED FROM: TrinoS3FileSystem
     * Helper function used to work around the fact that if you use an S3 bucket with an '_' that java.net.URI
     * behaves differently and sets the host value to null whereas S3 buckets without '_' have a properly
     * set host field. '_' is only allowed in S3 bucket names in us-east-1.
     *
     * @param uri The URI from which to extract a host value.
     * @return The host value where uri.getAuthority() is used when uri.getHost() returns null as long as no UserInfo is present.
     * @throws IllegalArgumentException If the bucket cannot be determined from the URI.
     */
    private static String extractBucketName(URI uri)
    {
        if (uri.getHost() != null) {
            return uri.getHost();
        }

        if (uri.getUserInfo() == null) {
            return uri.getAuthority();
        }

        throw new IllegalArgumentException("Unable to determine S3 bucket from URI.");
    }
}
