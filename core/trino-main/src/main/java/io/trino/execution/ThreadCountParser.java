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
package io.trino.execution;

import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.OptionalInt;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Suppliers.memoize;
import static java.lang.Long.parseLong;
import static java.lang.Math.min;
import static java.lang.Math.multiplyExact;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

// Based on https://github.com/airlift/units/pull/31, but adapted not to be a value class per https://github.com/trinodb/trino/pull/16303#issuecomment-1730146433
public class ThreadCountParser
{
    private static final String PER_CORE_SUFFIX = "C";
    private static final Supplier<Integer> AVAILABLE_PROCESSORS = memoize(MachineInfo::getAvailablePhysicalProcessorCount);
    public static final ThreadCountParser DEFAULT = new ThreadCountParser(AVAILABLE_PROCESSORS);

    private final Supplier<Integer> coreCount;

    @VisibleForTesting
    ThreadCountParser(Supplier<Integer> coreCount)
    {
        this.coreCount = requireNonNull(coreCount, "coreCount is null");
    }

    public int parse(String value)
    {
        int coreCount = this.coreCount.get();
        checkState(coreCount > 0, "coreCount must be positive");

        long threads;
        if (value.endsWith(PER_CORE_SUFFIX)) {
            long multiplier = parseLong(value.substring(0, value.length() - PER_CORE_SUFFIX.length()).trim());
            checkArgument(multiplier > 0, "Thread multiplier cannot be negative");
            threads = multiplyExact(multiplier, coreCount);
        }
        else {
            threads = parseLong(value);
        }

        checkArgument(threads <= Integer.MAX_VALUE, "Thread count is greater than 2^32 - 1");
        checkArgument(0 <= threads, "Thread count cannot be negative");
        return toIntExact(threads);
    }

    static final class MachineInfo
    {
        private MachineInfo() {}

        private static final Path CPU_INFO_PATH = Paths.get("/proc/cpuinfo");

        public static int getAvailablePhysicalProcessorCount()
        {
            String osArch = System.getProperty("os.arch");
            // logical core count (including container cpu quota if there is any)
            int availableProcessorCount = Runtime.getRuntime().availableProcessors();
            int totalPhysicalProcessorCount = availableProcessorCount;
            if ("amd64".equals(osArch) || "x86_64".equals(osArch)) {
                OptionalInt procInfo = tryReadFromProcCpuinfo();
                if (procInfo.isPresent()) {
                    totalPhysicalProcessorCount = procInfo.getAsInt();
                }
            }

            // cap available processor count to container cpu quota (if there is any).
            return min(totalPhysicalProcessorCount, availableProcessorCount);
        }

        private static OptionalInt tryReadFromProcCpuinfo()
        {
            if (!Files.exists(CPU_INFO_PATH)) {
                return OptionalInt.empty();
            }

            try (Stream<String> lines = Files.lines(CPU_INFO_PATH)) {
                return OptionalInt.of(toIntExact(lines.filter(line ->
                        line.matches("^processor\\s+: \\d")).count()));
            }
            catch (IOException e) {
                return OptionalInt.empty();
            }
        }
    }
}
