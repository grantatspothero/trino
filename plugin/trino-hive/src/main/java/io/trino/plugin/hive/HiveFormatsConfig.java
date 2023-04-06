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
package io.trino.plugin.hive;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

public class HiveFormatsConfig
{
    // TODO: Re-enable native readers and writers for Galaxy after they are vetted in the wild
    private boolean csvNativeReaderEnabled;
    private boolean csvNativeWriterEnabled;
    private boolean jsonNativeReaderEnabled;
    private boolean jsonNativeWriterEnabled;
    // Keep enabled for Galaxy because there is nothing to fallback to
    private boolean openXJsonNativeReaderEnabled = true;
    private boolean openXJsonNativeWriterEnabled = true;
    private boolean regexNativeReaderEnabled;
    private boolean textFileNativeReaderEnabled;
    private boolean textFileNativeWriterEnabled;
    private boolean sequenceFileNativeReaderEnabled;
    private boolean sequenceFileNativeWriterEnabled;

    public boolean isCsvNativeReaderEnabled()
    {
        return csvNativeReaderEnabled;
    }

    @Config("csv.native-reader.enabled")
    @ConfigDescription("Use native CSV reader")
    public HiveFormatsConfig setCsvNativeReaderEnabled(boolean csvNativeReaderEnabled)
    {
        this.csvNativeReaderEnabled = csvNativeReaderEnabled;
        return this;
    }

    public boolean isCsvNativeWriterEnabled()
    {
        return csvNativeWriterEnabled;
    }

    @Config("csv.native-writer.enabled")
    @ConfigDescription("Use native CSV writer")
    public HiveFormatsConfig setCsvNativeWriterEnabled(boolean csvNativeWriterEnabled)
    {
        this.csvNativeWriterEnabled = csvNativeWriterEnabled;
        return this;
    }

    public boolean isJsonNativeReaderEnabled()
    {
        return jsonNativeReaderEnabled;
    }

    @Config("json.native-reader.enabled")
    @ConfigDescription("Use native JSON reader")
    public HiveFormatsConfig setJsonNativeReaderEnabled(boolean jsonNativeReaderEnabled)
    {
        this.jsonNativeReaderEnabled = jsonNativeReaderEnabled;
        return this;
    }

    public boolean isJsonNativeWriterEnabled()
    {
        return jsonNativeWriterEnabled;
    }

    @Config("json.native-writer.enabled")
    @ConfigDescription("Use native JSON writer")
    public HiveFormatsConfig setJsonNativeWriterEnabled(boolean jsonNativeWriterEnabled)
    {
        this.jsonNativeWriterEnabled = jsonNativeWriterEnabled;
        return this;
    }

    public boolean isOpenXJsonNativeReaderEnabled()
    {
        return openXJsonNativeReaderEnabled;
    }

    @Config("openx-json.native-reader.enabled")
    @ConfigDescription("Use native OpenXJson reader")
    public HiveFormatsConfig setOpenXJsonNativeReaderEnabled(boolean openXJsonNativeReaderEnabled)
    {
        this.openXJsonNativeReaderEnabled = openXJsonNativeReaderEnabled;
        return this;
    }

    public boolean isOpenXJsonNativeWriterEnabled()
    {
        return openXJsonNativeWriterEnabled;
    }

    @Config("openx-json.native-writer.enabled")
    @ConfigDescription("Use native OpenXJson writer")
    public HiveFormatsConfig setOpenXJsonNativeWriterEnabled(boolean openXJsonNativeWriterEnabled)
    {
        this.openXJsonNativeWriterEnabled = openXJsonNativeWriterEnabled;
        return this;
    }

    public boolean isRegexNativeReaderEnabled()
    {
        return regexNativeReaderEnabled;
    }

    @Config("regex.native-reader.enabled")
    @ConfigDescription("Use native REGEX reader")
    public HiveFormatsConfig setRegexNativeReaderEnabled(boolean regexNativeReaderEnabled)
    {
        this.regexNativeReaderEnabled = regexNativeReaderEnabled;
        return this;
    }

    public boolean isTextFileNativeReaderEnabled()
    {
        return textFileNativeReaderEnabled;
    }

    @Config("text-file.native-reader.enabled")
    @ConfigDescription("Use native text file reader")
    public HiveFormatsConfig setTextFileNativeReaderEnabled(boolean textFileNativeReaderEnabled)
    {
        this.textFileNativeReaderEnabled = textFileNativeReaderEnabled;
        return this;
    }

    public boolean isTextFileNativeWriterEnabled()
    {
        return textFileNativeWriterEnabled;
    }

    @Config("text-file.native-writer.enabled")
    @ConfigDescription("Use native text file writer")
    public HiveFormatsConfig setTextFileNativeWriterEnabled(boolean textFileNativeWriterEnabled)
    {
        this.textFileNativeWriterEnabled = textFileNativeWriterEnabled;
        return this;
    }

    public boolean isSequenceFileNativeReaderEnabled()
    {
        return sequenceFileNativeReaderEnabled;
    }

    @Config("sequence-file.native-reader.enabled")
    @ConfigDescription("Use native sequence file reader")
    public HiveFormatsConfig setSequenceFileNativeReaderEnabled(boolean sequenceFileNativeReaderEnabled)
    {
        this.sequenceFileNativeReaderEnabled = sequenceFileNativeReaderEnabled;
        return this;
    }

    public boolean isSequenceFileNativeWriterEnabled()
    {
        return sequenceFileNativeWriterEnabled;
    }

    @Config("sequence-file.native-writer.enabled")
    @ConfigDescription("Use native sequence file writer")
    public HiveFormatsConfig setSequenceFileNativeWriterEnabled(boolean sequenceFileNativeWriterEnabled)
    {
        this.sequenceFileNativeWriterEnabled = sequenceFileNativeWriterEnabled;
        return this;
    }
}
