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
package io.trino.plugin.objectstore;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.ImmutableMap;
import io.trino.testing.containers.Minio;
import io.trino.util.AutoCloseableCloser;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class MinioStorage
        implements AutoCloseable
{
    public static final String ACCESS_KEY = "accesskey";
    public static final String SECRET_KEY = "secretkey";

    private final AutoCloseableCloser closer = AutoCloseableCloser.create();
    private final String bucketName;
    private final Minio minio;
    private AmazonS3 s3;

    public MinioStorage(String bucketName)
    {
        this.bucketName = requireNonNull(bucketName, "bucketName is null");
        this.minio = closer.register(Minio.builder()
                .withEnvVars(ImmutableMap.<String, String>builder()
                        .put("MINIO_ACCESS_KEY", ACCESS_KEY)
                        .put("MINIO_SECRET_KEY", SECRET_KEY)
                        .buildOrThrow())
                .build());
    }

    public void start()
    {
        minio.start();

        s3 = AmazonS3ClientBuilder.standard()
                .withPathStyleAccessEnabled(true)
                .withEndpointConfiguration(new EndpointConfiguration(getEndpoint(), null))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY)))
                .build();
        closer.register(s3::shutdown);

        s3.createBucket(bucketName);
    }

    @Override
    public void close()
            throws Exception
    {
        closer.close();
    }

    public List<String> listObjects(String key)
    {
        return s3.listObjects(bucketName, key).getObjectSummaries().stream()
                .map(S3ObjectSummary::getKey)
                .collect(toImmutableList());
    }

    public void deleteObjects(List<String> keys)
    {
        s3.deleteObjects(new DeleteObjectsRequest(bucketName)
                .withKeys(keys.toArray(String[]::new))
                .withQuiet(true));
    }

    public void putObject(String key, String content)
    {
        s3.putObject(bucketName, key, content);
    }

    @SuppressWarnings("HttpUrlsUsage")
    public String getEndpoint()
    {
        return "http://" + minio.getMinioApiEndpoint();
    }

    public String getS3Url()
    {
        return "s3://" + bucketName;
    }

    public Map<String, String> getHiveS3Config()
    {
        return ImmutableMap.<String, String>builder()
                .put("hive.s3.aws-access-key", ACCESS_KEY)
                .put("hive.s3.aws-secret-key", SECRET_KEY)
                .put("hive.s3.endpoint", getEndpoint())
                .put("hive.s3.path-style-access", "true")
                .buildOrThrow();
    }
}
