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
package org.apache.iceberg;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.TokenBuffer;
import com.google.errorprone.annotations.FormatMethod;
import io.airlift.log.Logger;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.util.JsonUtil;
import org.apache.iceberg.util.SerializableSupplier;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import static java.util.Objects.requireNonNull;

/**
 * Reads Iceberg metadata.json files with lazy snapshot loading.
 * <p>
 * {@link TableMetadataParser#read} parses the entire file into a Jackson tree and materializes
 * full TableMetadata including all N snapshots. This reader streams the file in a single
 * pass, eagerly loading only the ref'd snapshots (current + branches/tags) and deferring
 * the remaining historical snapshots to a lazy supplier via {@link TableMetadata#snapshots()}.
 * <p>
 * This follows the same pattern as Iceberg's REST catalog with {@code SnapshotMode.REFS}.
 */
public final class LazySnapshotTableMetadataReader
{
    private static final Logger log = Logger.get(LazySnapshotTableMetadataReader.class);
    private static final int MAX_SUPPORTED_FORMAT_VERSION = 3;

    private LazySnapshotTableMetadataReader() {}

    /**
     * Reads table metadata with only ref'd snapshots (current + branches/tags) eagerly loaded.
     * Unreferenced historical snapshots are loaded lazily on first access via
     * {@link TableMetadata#snapshots()} or {@link TableMetadata#snapshot(long)}.
     */
    public static TableMetadata readWithLazySnapshots(FileIO fileIO, String metadataLocation)
            throws IOException
    {
        TableMetadata refsOnly = readRefsOnlyTableMetadata(fileIO, metadataLocation);
        return TableMetadata.buildFrom(refsOnly)
                .withMetadataLocation(metadataLocation)
                .setPreviousFileLocation(null)
                .setSnapshotsSupplier((SerializableSupplier<List<Snapshot>>) () ->
                        TableMetadataParser.read(fileIO, metadataLocation).snapshots())
                .discardChanges()
                .build();
    }

    private static TableMetadata readRefsOnlyTableMetadata(FileIO fileIO, String metadataLocation)
            throws IOException
    {
        requireNonNull(fileIO, "fileIO is null");
        requireNonNull(metadataLocation, "metadataLocation is null");

        InputFile inputFile = fileIO.newInputFile(metadataLocation);
        TableMetadataParser.Codec codec = TableMetadataParser.Codec.fromFileName(metadataLocation);

        try (TableMetadataBuffers buffers = getTableMetadataBuffers(inputFile, codec)) {
            List<TokenBuffer> snapshotBuffers = resolveSnapshotBuffers(inputFile, codec, buffers);
            try (JsonParser mainParser = buffers.bufferWithoutSnapshots().asParser()) {
                ObjectNode mainNode = JsonUtil.mapper().readTree(mainParser);
                mainNode.set(TableMetadataParser.SNAPSHOTS, toArrayNode(snapshotBuffers));
                return TableMetadataParser.fromJson(metadataLocation, mainNode);
            }
        }
    }

    private static <T> T withRootObjectParser(InputFile inputFile, TableMetadataParser.Codec codec, ThrowingFunction<JsonParser, T, IOException> function)
            throws IOException
    {
        try (InputStream raw = inputFile.newStream();
                InputStream in = codec == TableMetadataParser.Codec.GZIP ? new GZIPInputStream(raw) : raw;
                JsonParser parser = JsonUtil.factory().createParser(in)) {
            parser.disable(JsonParser.Feature.AUTO_CLOSE_SOURCE);
            ensureCondition(parser.nextToken() == JsonToken.START_OBJECT, "Expected root JSON object in metadata file but found %s", parser.currentToken());
            T result = function.apply(parser);
            ensureCondition(parser.currentToken() == JsonToken.END_OBJECT, "Expected end of metadata file object");
            return result;
        }
    }

    private static TableMetadataBuffers getTableMetadataBuffers(InputFile inputFile, TableMetadataParser.Codec codec)
            throws IOException
    {
        return withRootObjectParser(inputFile, codec, LazySnapshotTableMetadataReader::findTableMetadataBuffers);
    }

    private static List<TokenBuffer> resolveSnapshotBuffers(
            InputFile inputFile,
            TableMetadataParser.Codec codec,
            TableMetadataBuffers buffers)
            throws IOException
    {
        if (buffers.unresolvedSnapshotIds().isEmpty()) {
            return new ArrayList<>(buffers.snapshotBuffers());
        }
        // If metadata file written using standard iceberg java writer, refs field exists before snapshots.
        // This allows single read pass optimization. But if non-standard field order, need second read pass.
        log.warn("Metadata file %s requires a second read pass to locate %s ref'd snapshot(s).",
                inputFile.location(),
                buffers.unresolvedSnapshotIds().size());
        List<TokenBuffer> result = new ArrayList<>(buffers.snapshotBuffers());
        result.addAll(findSnapshotBuffers(inputFile, codec, buffers.unresolvedSnapshotIds()));
        return result;
    }

    private static TableMetadataBuffers findTableMetadataBuffers(JsonParser parser)
            throws IOException
    {
        TokenBuffer bufferWithoutSnapshots = new TokenBuffer(parser);
        bufferWithoutSnapshots.writeStartObject();

        OptionalInt formatVersion = OptionalInt.empty();
        Set<Long> referencedSnapshotIds = new HashSet<>();
        List<TokenBuffer> snapshotBuffers = List.of();

        while (parser.nextToken() == JsonToken.FIELD_NAME) {
            String field = parser.currentName();
            parser.nextToken();

            switch (field) {
                case TableMetadataParser.FORMAT_VERSION -> {
                    ensureCondition(parser.currentToken() == JsonToken.VALUE_NUMBER_INT, "Expected integer value for required '%s' field but found %s", TableMetadataParser.FORMAT_VERSION, parser.currentToken());
                    formatVersion = OptionalInt.of(parser.getIntValue());
                    bufferWithoutSnapshots.writeFieldName(field);
                    bufferWithoutSnapshots.copyCurrentStructure(parser);
                }
                case TableMetadataParser.CURRENT_SNAPSHOT_ID -> {
                    ensureCondition(parser.currentToken() == JsonToken.VALUE_NULL || parser.currentToken() == JsonToken.VALUE_NUMBER_INT, "Current snapshot id must be null or an integer value but found %s", parser.currentToken().toString());
                    if (parser.currentToken() == JsonToken.VALUE_NUMBER_INT) {
                        long id = parser.getLongValue();
                        if (id != -1L) {
                            referencedSnapshotIds.add(id);
                        }
                    }
                    bufferWithoutSnapshots.writeFieldName(field);
                    bufferWithoutSnapshots.copyCurrentStructure(parser);
                }
                case TableMetadataParser.REFS -> {
                    bufferWithoutSnapshots.writeFieldName(field);
                    streamRefsAndExtractSnapshotIds(parser, bufferWithoutSnapshots, referencedSnapshotIds);
                }
                case TableMetadataParser.SNAPSHOTS -> {
                    if (!referencedSnapshotIds.isEmpty()) {
                        snapshotBuffers = scanArrayForMatchingEntries(parser, TableMetadataParser.SNAPSHOTS, referencedSnapshotIds);
                    }
                    else {
                        parser.skipChildren();
                    }
                }
                default -> {
                    bufferWithoutSnapshots.writeFieldName(field);
                    bufferWithoutSnapshots.copyCurrentStructure(parser);
                }
            }
        }

        bufferWithoutSnapshots.writeEndObject();
        ensureCondition(formatVersion.isPresent(), "Metadata file is missing required '%s' field", TableMetadataParser.FORMAT_VERSION);
        ensureCondition(formatVersion.getAsInt() <= MAX_SUPPORTED_FORMAT_VERSION, "Unsupported Iceberg %s %s; this reader only understands up to v%s", TableMetadataParser.FORMAT_VERSION, formatVersion.getAsInt(), MAX_SUPPORTED_FORMAT_VERSION);

        return new TableMetadataBuffers(bufferWithoutSnapshots, snapshotBuffers, referencedSnapshotIds);
    }

    private record TableMetadataBuffers(
            TokenBuffer bufferWithoutSnapshots,
            List<TokenBuffer> snapshotBuffers,
            Set<Long> unresolvedSnapshotIds)
            implements Closeable
    {
        public TableMetadataBuffers
        {
            requireNonNull(bufferWithoutSnapshots, "bufferWithoutSnapshots is null");
            requireNonNull(snapshotBuffers, "snapshotBuffers is null");
            requireNonNull(unresolvedSnapshotIds, "unresolvedSnapshotIds is null");
        }

        @Override
        public void close()
                throws IOException
        {
            bufferWithoutSnapshots.close();
            for (TokenBuffer buffer : snapshotBuffers) {
                buffer.close();
            }
        }
    }

    private static void streamRefsAndExtractSnapshotIds(JsonParser parser, TokenBuffer buffer, Set<Long> refSnapshotIds)
            throws IOException
    {
        ensureCondition(parser.currentToken() == JsonToken.START_OBJECT, "Expected refs to be an object but found %s", parser.currentToken());
        buffer.writeStartObject();
        while (parser.nextToken() == JsonToken.FIELD_NAME) {
            buffer.copyCurrentEvent(parser);
            parser.nextToken();
            ensureCondition(parser.currentToken() == JsonToken.START_OBJECT, "Expected ref entry to be an object but found %s", parser.currentToken());
            buffer.writeStartObject();
            while (parser.nextToken() == JsonToken.FIELD_NAME) {
                String refField = parser.currentName();
                buffer.writeFieldName(refField);
                parser.nextToken();
                if (TableMetadataParser.SNAPSHOT_ID.equals(refField)) {
                    refSnapshotIds.add(parser.getLongValue());
                }
                buffer.copyCurrentStructure(parser);
            }
            buffer.writeEndObject();
        }
        buffer.writeEndObject();
    }

    private static List<TokenBuffer> findSnapshotBuffers(InputFile inputFile, TableMetadataParser.Codec codec, Set<Long> snapshotIds)
            throws IOException
    {
        return withRootObjectParser(inputFile, codec, parser -> {
            List<TokenBuffer> result = List.of();
            while (parser.nextToken() == JsonToken.FIELD_NAME) {
                String field = parser.currentName();
                parser.nextToken();
                if (TableMetadataParser.SNAPSHOTS.equals(field)) {
                    result = scanArrayForMatchingEntries(parser, TableMetadataParser.SNAPSHOTS, snapshotIds);
                }
                else {
                    parser.skipChildren();
                }
            }
            return result;
        });
    }

    private static List<TokenBuffer> scanArrayForMatchingEntries(JsonParser parser, String fieldName, Set<Long> referencedSnapshotIds)
            throws IOException
    {
        ensureCondition(parser.currentToken() == JsonToken.START_ARRAY, "Expected array value for %s", fieldName);
        List<TokenBuffer> found = new ArrayList<>();
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            if (referencedSnapshotIds.isEmpty()) {
                parser.skipChildren();
                continue;
            }
            readEntryIfMatchingAnySnapshotId(parser, referencedSnapshotIds).ifPresent(found::add);
        }
        return found;
    }

    private static Optional<TokenBuffer> readEntryIfMatchingAnySnapshotId(JsonParser parser, Set<Long> referencedSnapshotIds)
            throws IOException
    {
        ensureCondition(parser.currentToken() == JsonToken.START_OBJECT, "Array entry must be a JSON object but found %s", parser.currentToken());
        TokenBuffer buffer = new TokenBuffer(parser);
        buffer.copyCurrentEvent(parser);
        boolean snapshotIdSeen = false;
        while (parser.nextToken() == JsonToken.FIELD_NAME) {
            String field = parser.currentName();
            buffer.copyCurrentEvent(parser);
            parser.nextToken();
            if (TableMetadataParser.SNAPSHOT_ID.equals(field)) {
                snapshotIdSeen = true;
                if (!referencedSnapshotIds.remove(parser.getLongValue())) {
                    while (parser.nextToken() == JsonToken.FIELD_NAME) {
                        parser.nextToken();
                        parser.skipChildren();
                    }
                    return Optional.empty();
                }
            }
            buffer.copyCurrentStructure(parser);
        }
        buffer.copyCurrentEvent(parser);
        ensureCondition(snapshotIdSeen, "Array entry is missing required '%s' field", TableMetadataParser.SNAPSHOT_ID);
        return Optional.of(buffer);
    }

    private static ArrayNode toArrayNode(List<TokenBuffer> buffers)
            throws IOException
    {
        ArrayNode array = JsonUtil.mapper().createArrayNode();
        for (TokenBuffer buffer : buffers) {
            try (JsonParser entryParser = buffer.asParser()) {
                array.add((JsonNode) JsonUtil.mapper().readTree(entryParser));
            }
        }
        return array;
    }

    @FormatMethod
    private static void ensureCondition(boolean condition, String messageTemplate, Object... args)
            throws IOException
    {
        if (!condition) {
            throw new IOException(String.format(messageTemplate, args));
        }
    }

    @FunctionalInterface
    interface ThrowingFunction<T, R, X extends Exception>
    {
        R apply(T t)
                throws X;
    }
}
