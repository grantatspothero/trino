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
package io.trino.spi.galaxy;

import io.trino.spi.TrinoException;

import java.util.concurrent.atomic.AtomicLong;

import static io.trino.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;

public class NetworkUsageQuotaEnforcer
{
    private final AtomicLong totalReadBytes = new AtomicLong();
    private final AtomicLong totalWriteBytes = new AtomicLong();

    public void recordReadBytesAndThrowIfLimitExceeded(long bytes, long readLimit)
    {
        if (totalReadBytes.addAndGet(bytes) > readLimit) {
            throw new TrinoException(
                    GENERIC_INSUFFICIENT_RESOURCES,
                    "Cross-region read data transfer limit of %sGB per worker exceeded. To increase this limit, contact Starburst support.".formatted(convertBytesToGigabytes(readLimit)));
        }
    }

    public void recordWriteBytesAndThrowIfLimitExceeded(long bytes, long writeLimit)
    {
        if (totalWriteBytes.addAndGet(bytes) > writeLimit) {
            throw new TrinoException(
                    GENERIC_INSUFFICIENT_RESOURCES,
                    "Cross-region write data transfer limit of %sGB per worker exceeded. To increase this limit, contact Starburst support.".formatted(convertBytesToGigabytes(writeLimit)));
        }
    }

    public void checkLimitsAndThrowIfExceeded(long readLimit, long writeLimit)
    {
        if (totalReadBytes.get() > readLimit && totalWriteBytes.get() > writeLimit) {
            throw new TrinoException(
                    GENERIC_INSUFFICIENT_RESOURCES,
                    "Cross-region read/write data transfer limits of %sGB / %sGB per worker exceeded. To increase these limits, contact Starburst support.".formatted(convertBytesToGigabytes(readLimit), convertBytesToGigabytes(writeLimit)));
        }
        if (totalReadBytes.get() > readLimit) {
            throw new TrinoException(
                    GENERIC_INSUFFICIENT_RESOURCES,
                    "Cross-region read data transfer limit of %sGB per worker exceeded. To increase this limit, contact Starburst support.".formatted(convertBytesToGigabytes(readLimit)));
        }
        if (totalWriteBytes.get() > writeLimit) {
            throw new TrinoException(
                    GENERIC_INSUFFICIENT_RESOURCES,
                    "Cross-region write data transfer limit of %sGB per worker exceeded. To increase this limit, contact Starburst support.".formatted(convertBytesToGigabytes(writeLimit)));
        }
    }

    private static long convertBytesToGigabytes(long numBytes)
    {
        return numBytes / 1_000_000_000;
    }
}
