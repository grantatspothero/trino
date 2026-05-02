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

import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.OptionalLong;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.nio.file.Files.writeString;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestLazySnapshotTableMetadataReader
{
    @ParameterizedTest(name = "{0}")
    @MethodSource("readTableMetadataCases")
    void testReadWithLazySnapshots(ReadTableMetadataCase testCase, @TempDir Path tempDir)
            throws IOException
    {
        Path metadataFile = tempDir.resolve(testCase.gzip() ? "00001-abc.gz.metadata.json" : "00001-abc.metadata.json");
        writeIcebergMetadata(metadataFile, testCase.startSnapshotId(), testCase.endSnapshotId(), testCase.currentSnapshotId());

        TableMetadata full = TableMetadataParser.read(localFileIO(), metadataFile.toString());
        TableMetadata lazy = LazySnapshotTableMetadataReader.readWithLazySnapshots(localFileIO(), metadataFile.toString());

        assertNonSnapshotFieldsMatch(lazy, full);

        if (testCase.currentSnapshotId().isPresent()) {
            long expectedId = testCase.currentSnapshotId().getAsLong();
            assertThat(lazy.currentSnapshot()).isNotNull();
            assertThat(lazy.currentSnapshot().snapshotId()).isEqualTo(expectedId);
            assertThat(lazy.currentSnapshot().manifestListLocation()).isEqualTo(full.currentSnapshot().manifestListLocation());
            assertThat(lazy.snapshots()).containsExactlyElementsOf(full.snapshots());
            assertThat(lazy.snapshotLog()).isEqualTo(full.snapshotLog());
        }
        else {
            assertThat(lazy.currentSnapshot()).isNull();
            assertThat(lazy.snapshots()).isEmpty();
            assertThat(lazy.snapshotLog()).isEqualTo(full.snapshotLog());
        }
    }

    static Stream<ReadTableMetadataCase> readTableMetadataCases()
    {
        return Stream.of(
                new ReadTableMetadataCase("current snapshot in 200 entries", 1L, 201L, OptionalLong.of(200L), false),
                new ReadTableMetadataCase("current snapshot in gzipped metadata", 1L, 101L, OptionalLong.of(100L), true),
                new ReadTableMetadataCase("no current snapshot (empty table)", 1L, 1L, OptionalLong.empty(), false));
    }

    record ReadTableMetadataCase(String label, long startSnapshotId, long endSnapshotId, OptionalLong currentSnapshotId, boolean gzip)
    {
        @Override
        public String toString()
        {
            return label;
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("noCurrentSnapshotCases")
    void testReadWithLazySnapshotsHandlesNoCurrentSnapshot(NoCurrentSnapshotCase testCase, @TempDir Path tempDir)
            throws IOException
    {
        Path metadataFile = tempDir.resolve("00001-abc.metadata.json");
        String json = """
                {
                  "format-version": 2,
                  "table-uuid": "00000000-0000-0000-0000-000000000001",
                  "location": "s3://bucket/table",
                  "last-updated-ms": 1700000000000,
                  "last-sequence-number": 0,
                  "last-column-id": 0,
                  "schemas": [{"type": "struct", "schema-id": 0, "fields": []}],
                  "current-schema-id": 0,
                  "partition-specs": [{"spec-id": 0, "fields": []}],
                  "default-spec-id": 0,
                  "last-partition-id": 999,
                  "sort-orders": [{"order-id": 0, "fields": []}],
                  "default-sort-order-id": 0,
                  "properties": {},
                  "current-snapshot-id": %s,
                  "snapshots": []
                }
                """.formatted(testCase.currentSnapshotIdJson());
        writeString(metadataFile, json);

        TableMetadata full = TableMetadataParser.read(localFileIO(), metadataFile.toString());
        TableMetadata lazy = LazySnapshotTableMetadataReader.readWithLazySnapshots(localFileIO(), metadataFile.toString());

        assertNonSnapshotFieldsMatch(lazy, full);
        assertThat(lazy.currentSnapshot()).isNull();
        assertThat(lazy.snapshots()).isEmpty();
    }

    static Stream<NoCurrentSnapshotCase> noCurrentSnapshotCases()
    {
        return Stream.of(
                new NoCurrentSnapshotCase("null (format v2/v3)", "null"),
                new NoCurrentSnapshotCase("-1 sentinel (format v1/v2)", "-1"));
    }

    record NoCurrentSnapshotCase(String label, String currentSnapshotIdJson)
    {
        @Override
        public String toString()
        {
            return label;
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidFormatVersionCases")
    void testRejectsInvalidFormatVersion(FormatVersionRejectionCase testCase, @TempDir Path tempDir)
            throws IOException
    {
        Path metadataFile = tempDir.resolve("00001-abc.metadata.json");
        writeString(metadataFile, testCase.metadataJson());

        assertThatThrownBy(() -> LazySnapshotTableMetadataReader.readWithLazySnapshots(localFileIO(), metadataFile.toString()))
                .isInstanceOf(IOException.class)
                .hasMessage(testCase.expectedMessage());
    }

    static Stream<FormatVersionRejectionCase> invalidFormatVersionCases()
    {
        String missingVersionJson = """
                {
                  "table-uuid": "00000000-0000-0000-0000-000000000001",
                  "location": "s3://bucket/table",
                  "current-snapshot-id": null,
                  "snapshots": []
                }
                """;
        String unsupportedVersionJson = """
                {
                  "format-version": 4,
                  "table-uuid": "00000000-0000-0000-0000-000000000001",
                  "location": "s3://bucket/table",
                  "current-snapshot-id": null,
                  "snapshots": []
                }
                """;
        String missingMessage = "Metadata file is missing required 'format-version' field";
        String unsupportedMessage = "Unsupported Iceberg format-version 4; this reader only understands up to v3";
        return Stream.of(
                new FormatVersionRejectionCase(missingVersionJson, missingMessage),
                new FormatVersionRejectionCase(unsupportedVersionJson, unsupportedMessage));
    }

    record FormatVersionRejectionCase(String metadataJson, String expectedMessage)
    {
        @Override
        public String toString()
        {
            return expectedMessage;
        }
    }

    @Test
    void testReadWithLazySnapshotsHandlesSnapshotsBeforeCurrentSnapshotId(@TempDir Path tempDir)
            throws IOException
    {
        Path metadataFile = tempDir.resolve("00001-abc.metadata.json");
        long targetSnapshotId = 555L;
        String json = """
                {
                  "format-version": 1,
                  "table-uuid": "00000000-0000-0000-0000-000000000001",
                  "location": "s3://bucket/table",
                  "last-updated-ms": 1700000000000,
                  "last-column-id": 0,
                  "schema": {"type": "struct", "schema-id": 0, "fields": []},
                  "partition-spec": [],
                  "properties": {},
                  "snapshots": [%s],
                  "current-snapshot-id": %d
                }
                """.formatted(snapshotJsonV1(targetSnapshotId), targetSnapshotId);
        writeString(metadataFile, json);

        CountingFileIO fileIO = new CountingFileIO(localFileIO());
        TableMetadata full = TableMetadataParser.read(localFileIO(), metadataFile.toString());
        TableMetadata lazy = LazySnapshotTableMetadataReader.readWithLazySnapshots(fileIO, metadataFile.toString());

        assertNonSnapshotFieldsMatch(lazy, full);
        assertThat(lazy.currentSnapshot()).isNotNull();
        assertThat(lazy.currentSnapshot().snapshotId()).isEqualTo(targetSnapshotId);
        // Two reads: one for the initial parse (snapshots before current-snapshot-id triggers second pass),
        // and the readWithLazySnapshots wrapper itself reads once
        assertThat(fileIO.streamOpenCount()).isEqualTo(2);
    }

    @Test
    void testNonTrivialMetadataFieldsRoundtrip(@TempDir Path tempDir)
            throws IOException
    {
        Path metadataFile = tempDir.resolve("00001-abc.metadata.json");
        long oldSnapshotId = 41L;
        long currentSnapshotId = 42L;

        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "category", Types.StringType.get()),
                Types.NestedField.optional(3, "amount", Types.DoubleType.get()),
                Types.NestedField.optional(4, "event_date", Types.DateType.get()),
                Types.NestedField.optional(5, "event_ts", Types.TimestampType.withZone()));
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .identity("category")
                .bucket("id", 16)
                .year("event_date")
                .build();
        SortOrder sortOrder = SortOrder.builderFor(schema)
                .sortBy("id", SortDirection.ASC, NullOrder.NULLS_LAST)
                .sortBy("category", SortDirection.DESC, NullOrder.NULLS_FIRST)
                .build();
        Map<String, String> properties = Map.of(
                "write.parquet.compression-codec", "zstd",
                "read.split.target-size", "134217728");
        TableMetadata initial = TableMetadata.newTableMetadata(schema, spec, sortOrder, "s3://bucket/table", properties);
        TableMetadata.Builder builder = TableMetadata.buildFrom(initial);
        builder.addSnapshot(buildSnapshot(oldSnapshotId, 1));
        builder.setBranchSnapshot(oldSnapshotId, SnapshotRef.MAIN_BRANCH);
        builder.setRef("audit-tag", SnapshotRef.tagBuilder(oldSnapshotId).build());
        builder.addSnapshot(buildSnapshot(currentSnapshotId, 2));
        builder.setBranchSnapshot(currentSnapshotId, SnapshotRef.MAIN_BRANCH);
        TableMetadataParser.overwrite(builder.build(), Files.localOutput(metadataFile.toFile()));

        TableMetadata full = TableMetadataParser.read(localFileIO(), metadataFile.toString());
        TableMetadata lazy = LazySnapshotTableMetadataReader.readWithLazySnapshots(localFileIO(), metadataFile.toString());

        assertNonSnapshotFieldsMatch(lazy, full);
        assertThat(lazy.currentSnapshot()).isNotNull();
        assertThat(lazy.currentSnapshot()).isEqualTo(full.currentSnapshot());
        assertThat(lazy.currentSnapshot().snapshotId()).isEqualTo(currentSnapshotId);

        // All refs are preserved with their ref'd snapshots eagerly loaded (SnapshotMode.REFS behavior)
        assertThat(lazy.refs()).isEqualTo(full.refs());
        assertThat(lazy.refs()).containsKeys(SnapshotRef.MAIN_BRANCH, "audit-tag");
        assertThat(lazy.snapshot(oldSnapshotId)).isEqualTo(full.snapshot(oldSnapshotId));

        // After lazy load triggers, all snapshots are accessible and refs remain consistent
        assertThat(lazy.snapshots()).containsExactlyElementsOf(full.snapshots());
        assertThat(lazy.snapshot(oldSnapshotId)).isEqualTo(full.snapshot(oldSnapshotId));
        assertThat(lazy.snapshot(currentSnapshotId)).isEqualTo(full.snapshot(currentSnapshotId));
        assertThat(lazy.refs()).isEqualTo(full.refs());
        assertThat(lazy.snapshotLog()).isEqualTo(full.snapshotLog());
    }

    @Test
    void testLazySnapshotSupplierTriggersOnSnapshotsAccess(@TempDir Path tempDir)
            throws IOException
    {
        Path metadataFile = tempDir.resolve("00001-abc.metadata.json");
        writeIcebergMetadata(metadataFile, 1L, 11L, OptionalLong.of(10L));

        CountingFileIO fileIO = new CountingFileIO(localFileIO());
        TableMetadata lazy = LazySnapshotTableMetadataReader.readWithLazySnapshots(fileIO, metadataFile.toString());

        assertThat(lazy.currentSnapshot()).isNotNull();
        assertThat(lazy.currentSnapshot().snapshotId()).isEqualTo(10L);
        int readsBeforeSnapshotsAccess = fileIO.streamOpenCount();

        // Accessing snapshots() triggers the lazy supplier which re-reads the full metadata
        assertThat(lazy.snapshots()).hasSize(10);
        assertThat(fileIO.streamOpenCount()).isGreaterThan(readsBeforeSnapshotsAccess);
    }

    @Test
    public void testLargeSnapshotArray(@TempDir Path tempDir)
            throws IOException
    {
        long startSnapshotId = 1L;
        long endSnapshotId = 200_000L;
        long targetSnapshotId = 100_000L;
        Path metadataFile = tempDir.resolve("00001-abc.metadata.json");
        writeIcebergMetadata(metadataFile, startSnapshotId, endSnapshotId, OptionalLong.of(targetSnapshotId));

        TableMetadata icebergParser = TableMetadataParser.read(localFileIO(), metadataFile.toString());
        TableMetadata lazy = LazySnapshotTableMetadataReader.readWithLazySnapshots(localFileIO(), metadataFile.toString());
        assertNonSnapshotFieldsMatch(lazy, icebergParser);
        assertThat(lazy.currentSnapshot()).isNotNull();
        assertThat(lazy.currentSnapshot().snapshotId()).isEqualTo(targetSnapshotId);
        assertThat(lazy.currentSnapshot().manifestListLocation()).isEqualTo(icebergParser.currentSnapshot().manifestListLocation());
        assertThat(lazy.snapshotLog()).isEqualTo(icebergParser.snapshotLog());
    }

    private static void writeIcebergMetadata(
            Path path,
            long startSnapshotId,
            long endSnapshotId,
            OptionalLong currentSnapshotId)
    {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
        PartitionSpec spec = PartitionSpec.unpartitioned();
        TableMetadata empty = TableMetadata.newTableMetadata(schema, spec, "s3://bucket/table", Map.of());
        TableMetadata.Builder builder = TableMetadata.buildFrom(empty);
        long sequenceNumber = 1;
        for (long snapshotId = startSnapshotId; snapshotId < endSnapshotId; snapshotId++) {
            builder.addSnapshot(buildSnapshot(snapshotId, sequenceNumber++));
        }
        if (currentSnapshotId.isPresent()) {
            builder.setBranchSnapshot(currentSnapshotId.getAsLong(), SnapshotRef.MAIN_BRANCH);
        }
        TableMetadataParser.overwrite(builder.build(), Files.localOutput(path.toFile()));
    }

    private static Snapshot buildSnapshot(long snapshotId, long sequenceNumber)
    {
        return SnapshotParser.fromJson(snapshotJson(snapshotId, sequenceNumber));
    }

    private static String snapshotJson(long snapshotId, long sequenceNumber)
    {
        return """
                {
                  "sequence-number": %d,
                  "snapshot-id": %d,
                  "timestamp-ms": 1700000000000,
                  "summary": {"operation": "append", "added-files-size": "12345", "total-records": "100"},
                  "manifest-list": "s3://bucket/manifests/%d.avro",
                  "schema-id": 0
                }
                """.formatted(sequenceNumber, snapshotId, snapshotId);
    }

    private static String snapshotJsonV1(long snapshotId)
    {
        return """
                {
                  "snapshot-id": %d,
                  "timestamp-ms": 1700000000000,
                  "summary": {"operation": "append"},
                  "manifest-list": "s3://bucket/manifests/%d.avro",
                  "schema-id": 0
                }
                """.formatted(snapshotId, snapshotId);
    }

    private static void assertNonSnapshotFieldsMatch(TableMetadata lazy, TableMetadata full)
    {
        assertThat(lazy.formatVersion()).isEqualTo(full.formatVersion());
        assertThat(lazy.uuid()).isEqualTo(full.uuid());
        assertThat(lazy.location()).isEqualTo(full.location());
        assertThat(lazy.lastUpdatedMillis()).isEqualTo(full.lastUpdatedMillis());
        assertThat(lazy.lastSequenceNumber()).isEqualTo(full.lastSequenceNumber());
        assertThat(lazy.lastColumnId()).isEqualTo(full.lastColumnId());
        assertThat(lazy.schemas().stream().map(Schema::asStruct)).containsExactlyElementsOf(full.schemas().stream().map(Schema::asStruct).collect(toImmutableList()));
        assertThat(lazy.currentSchemaId()).isEqualTo(full.currentSchemaId());
        assertThat(lazy.specs()).isEqualTo(full.specs());
        assertThat(lazy.defaultSpecId()).isEqualTo(full.defaultSpecId());
        assertThat(lazy.lastAssignedPartitionId()).isEqualTo(full.lastAssignedPartitionId());
        assertThat(lazy.sortOrders()).isEqualTo(full.sortOrders());
        assertThat(lazy.defaultSortOrderId()).isEqualTo(full.defaultSortOrderId());
        assertThat(lazy.properties()).isEqualTo(full.properties());
        assertThat(lazy.refs()).isEqualTo(full.refs());
        assertThat(lazy.snapshotLog()).isEqualTo(full.snapshotLog());
        assertThat(lazy.previousFiles()).isEqualTo(full.previousFiles());
        assertThat(lazy.statisticsFiles()).isEqualTo(full.statisticsFiles());
        assertThat(lazy.partitionStatisticsFiles()).isEqualTo(full.partitionStatisticsFiles());
        assertThat(lazy.encryptionKeys()).isEqualTo(full.encryptionKeys());
    }

    private static FileIO localFileIO()
    {
        return new FileIO()
        {
            @Override
            public InputFile newInputFile(String path)
            {
                return Files.localInput(path);
            }

            @Override
            public OutputFile newOutputFile(String path)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void deleteFile(String path)
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    private static final class CountingFileIO
            implements FileIO
    {
        private final FileIO delegate;
        private int streamOpenCount;

        public CountingFileIO(FileIO delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate");
        }

        public int streamOpenCount()
        {
            return streamOpenCount;
        }

        @Override
        public InputFile newInputFile(String path)
        {
            InputFile delegateFile = delegate.newInputFile(path);
            return new InputFile()
            {
                @Override
                public long getLength()
                {
                    return delegateFile.getLength();
                }

                @Override
                public SeekableInputStream newStream()
                {
                    streamOpenCount++;
                    return delegateFile.newStream();
                }

                @Override
                public String location()
                {
                    return delegateFile.location();
                }

                @Override
                public boolean exists()
                {
                    return delegateFile.exists();
                }
            };
        }

        @Override
        public OutputFile newOutputFile(String path)
        {
            return delegate.newOutputFile(path);
        }

        @Override
        public void deleteFile(String path)
        {
            delegate.deleteFile(path);
        }
    }
}
