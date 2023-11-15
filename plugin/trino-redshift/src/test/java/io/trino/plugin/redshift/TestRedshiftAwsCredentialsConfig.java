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
package io.trino.plugin.redshift;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestRedshiftAwsCredentialsConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(RedshiftAwsCredentialsConfig.class)
                .setConnectionUser(null)
                .setRegionName(null)
                .setAccessKey(null)
                .setSecretKey(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("connection-user", "dbuser")
                .put("aws.region-name", "us-east-2")
                .put("aws.access-key", "accessKey")
                .put("aws.secret-key", "secretKey")
                .buildOrThrow();

        RedshiftAwsCredentialsConfig expected = new RedshiftAwsCredentialsConfig()
                .setConnectionUser("dbuser")
                .setRegionName("us-east-2")
                .setAccessKey("accessKey")
                .setSecretKey("secretKey");

        assertFullMapping(properties, expected);
    }
}
