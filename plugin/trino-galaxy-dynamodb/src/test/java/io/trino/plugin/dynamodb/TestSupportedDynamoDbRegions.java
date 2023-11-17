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
package io.trino.plugin.dynamodb;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.starburstdata.trino.plugins.dynamodb.DynamoDbConnectionFactory.AWS_REGION_TO_CDATA_REGION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestSupportedDynamoDbRegions
{
    @Test // This test ensures that Galaxy shows only driver supported regions.
    public void testSupportedDynamoDbRegions()
    {
        Map<String, String> actualSupportedRegions = AWS_REGION_TO_CDATA_REGION;
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        Map<String, String> expectedSupportedRegions =
                builder.put("us-east-2", "OHIO")
                        .put("us-east-1", "NORTHERNVIRGINIA")
                        .put("us-west-1", "NORTHERNCALIFORNIA")
                        .put("us-west-2", "OREGON")
                        .put("af-south-1", "CAPETOWN")
                        .put("ap-east-1", "HONGKONG")
                        .put("ap-south-1", "MUMBAI")
                        .put("ap-northeast-3", "OSAKA")
                        .put("ap-northeast-2", "SEOUL")
                        .put("ap-southeast-1", "SINGAPORE")
                        .put("ap-southeast-2", "SYDNEY")
                        .put("ap-southeast-3", "JAKARTA")
                        .put("ap-northeast-1", "TOKYO")
                        .put("ca-central-1", "CENTRAL")
                        .put("cn-north-1", "BEIJING")
                        .put("us-iso-east-1", "NINGXIA")
                        .put("eu-central-1", "FRANKFURT")
                        .put("eu-central-2", "ZURICH")
                        .put("eu-west-1", "IRELAND")
                        .put("eu-west-2", "LONDON")
                        .put("eu-south-1", "MILAN")
                        .put("eu-west-3", "PARIS")
                        .put("eu-north-1", "STOCKHOLM")
                        .put("me-south-1", "BAHRAIN")
                        .put("me-central-1", "UAE")
                        .put("sa-east-1", "SAOPAULO")
                        .put("us-gov-east-1", "GOVCLOUDEAST")
                        .put("us-gov-west-1", "GOVCLOUDWEST")
                        .buildOrThrow();

        for (Map.Entry<String, String> expectedEntry : expectedSupportedRegions.entrySet()) {
            assertTrue(actualSupportedRegions.containsKey(expectedEntry.getKey()));
            assertEquals(expectedEntry.getValue(), actualSupportedRegions.get(expectedEntry.getKey()));
        }
    }
}
