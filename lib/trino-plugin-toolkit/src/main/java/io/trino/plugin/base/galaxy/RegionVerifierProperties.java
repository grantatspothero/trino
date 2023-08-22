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
package io.trino.plugin.base.galaxy;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public record RegionVerifierProperties(
        boolean crossRegionAllowed,
        List<String> localRegionIpAddresses)
{
    public static final String CROSS_REGION_ALLOWED_PROPERTY_NAME = "crossRegionAllowed";
    public static final String LOCAL_REGION_IP_ADDRESSES_PROPERTY_NAME = "localRegionIpAddresses";
    private static final char IP_ADDRESSES_SEPARATOR = ',';

    public RegionVerifierProperties
    {
        requireNonNull(localRegionIpAddresses, "localRegionIpAddresses is null");
    }

    public static RegionVerifierProperties generateFrom(RegionEnforcementConfig config)
    {
        return new RegionVerifierProperties(config.getAllowCrossRegionAccess(), config.getAllowedIpAddresses());
    }

    public static void addRegionVerifierProperties(BiConsumer<String, String> propertiesConsumer, RegionVerifierProperties regionVerifierProperties)
    {
        propertiesConsumer.accept(CROSS_REGION_ALLOWED_PROPERTY_NAME, String.valueOf(regionVerifierProperties.crossRegionAllowed()));
        propertiesConsumer.accept(LOCAL_REGION_IP_ADDRESSES_PROPERTY_NAME, Joiner.on(IP_ADDRESSES_SEPARATOR).join(regionVerifierProperties.localRegionIpAddresses()));
    }

    public static RegionVerifierProperties getRegionVerifierProperties(Function<String, String> propertyProvider)
    {
        boolean crossRegionAllowed = Boolean.parseBoolean(getRequiredProperty(propertyProvider, CROSS_REGION_ALLOWED_PROPERTY_NAME));
        List<String> localRegionIpAddresses = Splitter.on(IP_ADDRESSES_SEPARATOR).splitToList(getRequiredProperty(propertyProvider, LOCAL_REGION_IP_ADDRESSES_PROPERTY_NAME));
        return new RegionVerifierProperties(crossRegionAllowed, localRegionIpAddresses);
    }

    private static String getRequiredProperty(Function<String, String> propertyProvider, String propertyName)
    {
        return Optional.ofNullable(propertyProvider.apply(propertyName))
                .orElseThrow(() -> new IllegalArgumentException("Missing required property: " + propertyName));
    }
}
