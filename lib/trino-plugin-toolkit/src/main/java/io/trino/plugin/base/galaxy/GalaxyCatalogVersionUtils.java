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

import io.starburst.stargate.id.CatalogId;
import io.trino.spi.connector.CatalogHandle;

import java.util.HexFormat;
import java.util.Optional;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * Utilities for storing galaxy-specific information in the CatalogVersion string
 * Different CatalogMangerKinds need to store different information in the version string:
 * - DYNAMIC not supported in galaxy
 * - STATIC/LIVE/METADATA_ONLY: catalogId
 * - STATIC: transactionId
 * - LIVE: liveCatalogUniqueId
 * Since CatalogVersion#isAllowedCharacter limits the allowed alphabet of CatalogVersion,
 * when storing catalogId/transactionId/uniqueId we lowercase HEX encode the string then store the encoded version string
 */
public final class GalaxyCatalogVersionUtils
{
    private GalaxyCatalogVersionUtils() {}

    private static final String STATIC_CATALOG_VERSION = "V1";
    private static final String METADATA_CATALOG_VERSION = "V2";
    private static final String LIVE_CATALOG_VERSION = "V3";

    public static CatalogHandle.CatalogVersion fromCatalogId(CatalogId catalogId)
    {
        return new CatalogHandle.CatalogVersion(toLowercaseHex(STATIC_CATALOG_VERSION + catalogId.toString() + ";"));
    }

    public static CatalogHandle.CatalogVersion fromCatalogIdAndLiveCatalogUniqueId(CatalogId catalogId, UUID liveCatalogUniqueId)
    {
        return new CatalogHandle.CatalogVersion(toLowercaseHex(LIVE_CATALOG_VERSION + catalogId.toString() + ";" + liveCatalogUniqueId.toString() + ";"));
    }

    public static CatalogHandle.CatalogVersion fromCatalogIdAndTransactionId(CatalogId catalogId, UUID transactionId)
    {
        return new CatalogHandle.CatalogVersion(toLowercaseHex(METADATA_CATALOG_VERSION + catalogId.toString() + ";" + transactionId.toString() + ";"));
    }

    public static CatalogId getRequiredCatalogId(CatalogHandle.CatalogVersion version)
    {
        return GalaxyCatalogVersion.fromCatalogVersion(version).catalogId();
    }

    public static UUID getRequiredTransactionId(CatalogHandle.CatalogVersion version)
    {
        return GalaxyCatalogVersion.fromCatalogVersion(version).transactionId().orElseThrow(() ->
                new RuntimeException("CatalogHandle.version: %s is invalid, must contain a transaction id".formatted(version.toString())));
    }

    public static UUID getRequiredLiveCatalogUniqueId(CatalogHandle.CatalogVersion version)
    {
        return GalaxyCatalogVersion.fromCatalogVersion(version).liveCatalogUniqueId().orElseThrow(() ->
                new RuntimeException("CatalogHandle.version: %s is invalid, must contain a live catalog unique id".formatted(version.toString())));
    }

    private static Optional<String> fromLowercaseHex(String value)
    {
        try {
            return Optional.of(new String(HexFormat.of().withLowerCase().parseHex(value), UTF_8));
        }
        catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    private static String toLowercaseHex(String value)
    {
        return HexFormat.of().withLowerCase().formatHex(value.getBytes(UTF_8));
    }

    public record GalaxyCatalogVersion(
            String decodedVersion,
            CatalogId catalogId,
            Optional<UUID> transactionId,
            Optional<UUID> liveCatalogUniqueId)
    {
        public GalaxyCatalogVersion
        {
            requireNonNull(decodedVersion, "decodedVersion is null");
            requireNonNull(catalogId, "catalogId is null");
            requireNonNull(transactionId, "transactionId is null");
            requireNonNull(liveCatalogUniqueId, "liveCatalogUniqueId is null");
        }

        public static GalaxyCatalogVersion fromCatalogVersion(CatalogHandle.CatalogVersion version)
        {
            String decodedVersion = fromLowercaseHex(version.toString()).orElseThrow(() -> new IllegalArgumentException("Invalidly encoded galaxy catalog version"));
            return switch (decodedVersion.substring(0, 2)) {
                case STATIC_CATALOG_VERSION -> new GalaxyCatalogVersion(
                        decodedVersion,
                        new CatalogId(decodedVersion.substring(2, decodedVersion.indexOf(';'))),
                        Optional.empty(),
                        Optional.empty());
                case METADATA_CATALOG_VERSION -> new GalaxyCatalogVersion(
                        decodedVersion,
                        new CatalogId(decodedVersion.substring(2, decodedVersion.indexOf(';'))),
                        Optional.of(UUID.fromString(decodedVersion.substring(decodedVersion.indexOf(';') + 1, decodedVersion.lastIndexOf(';')))),
                        Optional.empty());
                case LIVE_CATALOG_VERSION -> new GalaxyCatalogVersion(
                        decodedVersion,
                        new CatalogId(decodedVersion.substring(2, decodedVersion.indexOf(';'))),
                        Optional.empty(),
                        Optional.of(UUID.fromString(decodedVersion.substring(decodedVersion.indexOf(';') + 1, decodedVersion.lastIndexOf(';')))));
                default -> throw new IllegalArgumentException("Unrecognized Galaxy Catalog Version");
            };
        }
    }
}
