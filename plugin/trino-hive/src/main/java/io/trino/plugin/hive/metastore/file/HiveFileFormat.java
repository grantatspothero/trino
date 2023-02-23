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
package io.trino.plugin.hive.metastore.file;

import io.trino.plugin.hive.HiveStorageFormat;

public enum HiveFileFormat
{
    ORC,
    PARQUET,
    AVRO,
    RCBINARY,
    RCTEXT,
    SEQUENCEFILE,
    JSON,
    TEXTFILE,
    CSV,
    REGEX,
    HUDI;

    HiveStorageFormat toHiveStorageFormat()
    {
        return switch (this) {
            case ORC -> HiveStorageFormat.ORC;
            case PARQUET -> HiveStorageFormat.PARQUET;
            case AVRO -> HiveStorageFormat.AVRO;
            case RCBINARY -> HiveStorageFormat.RCBINARY;
            case RCTEXT -> HiveStorageFormat.RCTEXT;
            case SEQUENCEFILE -> HiveStorageFormat.SEQUENCEFILE;
            case JSON -> HiveStorageFormat.JSON;
            case TEXTFILE -> HiveStorageFormat.TEXTFILE;
            case CSV -> HiveStorageFormat.CSV;
            case REGEX -> HiveStorageFormat.REGEX;
            case HUDI -> throw new UnsupportedOperationException("HUDI");
        };
    }

    public static HiveFileFormat fromHiveStorageFormat(HiveStorageFormat format)
    {
        return switch (format) {
            case ORC -> ORC;
            case PARQUET -> PARQUET;
            case AVRO -> AVRO;
            case RCBINARY -> RCBINARY;
            case RCTEXT -> RCTEXT;
            case SEQUENCEFILE -> SEQUENCEFILE;
            case JSON -> JSON;
            case TEXTFILE -> TEXTFILE;
            case CSV -> CSV;
            case REGEX -> REGEX;
        };
    }
}
