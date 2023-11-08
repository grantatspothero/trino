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

import io.trino.filesystem.Location;
import io.trino.filesystem.s3.S3PathUtils;
import io.trino.hdfs.s3.TrinoS3FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.filesystem.hdfs.HadoopPaths.hadoopPath;
import static io.trino.hdfs.s3.TrinoS3FileSystem.keysFromPath;
import static io.trino.hdfs.s3.TrinoS3FileSystem.legacyCorruptedKeyFromPath;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestLegacyCorruptedPaths
{
    @Test
    public void testLegacyCorruptedKey()
    {
        // s3a, s3n (never affected and thus ignored by legacy path handling)
        assertLegacyCorruptedKey("s3a", "some//path", Optional.of("some/path#%2Fsome%2F%2Fpath"));
        assertLegacyCorruptedKey("s3n", "some//path", Optional.of("some/path#%2Fsome%2F%2Fpath"));

        // no slashes
        assertLegacyCorruptedKey("s3", "some-path", Optional.empty());

        // single slashes
        assertLegacyCorruptedKey("s3", "some/path", Optional.empty());
        assertLegacyCorruptedKey("s3", "some/longer/path", Optional.empty());

        // double slash
        assertLegacyCorruptedKey("s3", "some//path", Optional.of("some/path#%2Fsome%2F%2Fpath"));

        // percent
        assertLegacyCorruptedKey("s3", "some%path", Optional.empty());

        // question mark
        assertLegacyCorruptedKey("s3", "some?path", Optional.empty());

        // initial slash (so double slash when combined with <bucket>/ prefix)
        assertLegacyCorruptedKey("s3", "/some/path", Optional.of("some/path#%2F%2Fsome%2Fpath"));

        assertLegacyCorruptedKey("s3", "//", Optional.of("#%2F%2F%2F"));
    }

    private static void assertLegacyCorruptedKey(String protocol, String correctKey, Optional<String> expected)
    {
        assertThat(legacyCorruptedKeyFromPath(new Path(format("%s://my-bucket/%s", protocol, correctKey)), correctKey, true))
                .isEqualTo(expected);

        assertThat(S3PathUtils.keysFromPath(correctKey, true).legacyCorruptedKey())
                .isEqualTo(expected);
    }

    @Test
    public void testKeysFromPath()
    {
        // s3a:// and s3n:// did not go through path corruption, since the original code checked for s3://
        // https://github.com/trinodb/trino/blob/46e215294bb01917ddd2bd7ce085a2f2d2cad8a4/lib/trino-hdfs/src/main/java/io/trino/filesystem/hdfs/HadoopPaths.java#L28-L41
        assertThat(keysFromPath(hadoopPath(Location.of("s3a://my-bucket/some//path")), true))
                .isEqualTo(new TrinoS3FileSystem.PathKeys(
                        "some/path",
                        Optional.empty()));

        // Double slash
        assertThat(keysFromPath(hadoopPath(Location.of("s3://my-bucket/some//path")), true))
                .isEqualTo(new TrinoS3FileSystem.PathKeys(
                        "some//path",
                        Optional.of("some/path#%2Fsome%2F%2Fpath")));

        // Double slash with supportLegacyCorruptedPaths=false
        assertThat(keysFromPath(hadoopPath(Location.of("s3://my-bucket/some//path")), false))
                .isEqualTo(new TrinoS3FileSystem.PathKeys(
                        "some//path",
                        Optional.empty()));

        // Percent
        assertThat(keysFromPath(hadoopPath(Location.of("s3://my-bucket/some%path")), true))
                .isEqualTo(new TrinoS3FileSystem.PathKeys(
                        "some%path",
                        Optional.empty()));
    }
}
