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
package io.trino.plugin.hive.functions;

import com.google.common.collect.ImmutableMap;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.plugin.base.galaxy.CrossRegionConfig;
import io.trino.plugin.base.galaxy.LocalRegionConfig;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.spi.connector.CatalogHandle;
import io.trino.testing.QueryRunner;

import java.io.IOException;

public class TestUnloadS3
        extends BaseUnloadFileSystemTest
{
    private static final String bucketName = requireEnv("S3_BUCKET");

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                .setHiveProperties(ImmutableMap.<String, String>builder()
                        .put("hive.s3.region", requireEnv("AWS_REGION"))
                        .put("hive.s3.aws-access-key", requireEnv("AWS_ACCESS_KEY_ID"))
                        .put("hive.s3.aws-secret-key", requireEnv("AWS_SECRET_ACCESS_KEY"))
                        .buildOrThrow())
                .build();
    }

    @Override
    protected TrinoFileSystemFactory getFileSystemFactory()
            throws IOException
    {
        return new S3FileSystemFactory(
                OpenTelemetry.noop(),
                new S3FileSystemConfig()
                        .setRegion(requireEnv("AWS_REGION"))
                        .setAwsAccessKey(requireEnv("AWS_ACCESS_KEY_ID"))
                        .setAwsSecretKey(requireEnv("AWS_SECRET_ACCESS_KEY")),
                CatalogHandle.fromId("catalog:normal:1"),
                new LocalRegionConfig(),
                new CrossRegionConfig());
    }

    @Override
    protected String getLocation(String path)
    {
        return "s3://%s/%s".formatted(bucketName, path);
    }
}
