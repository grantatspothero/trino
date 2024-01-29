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

import com.google.errorprone.annotations.FormatMethod;

import static java.util.Objects.requireNonNull;

public class QuotaEnforcingNetworkMonitor
        extends NetworkMonitor
{
    private final NetworkUsageQuotaEnforcer networkUsageQuotaEnforcer;
    private final long maxReadBytes;
    private final long maxWriteBytes;

    public QuotaEnforcingNetworkMonitor(NetworkUsageQuotaEnforcer networkUsageQuotaEnforcer, long maxReadBytes, long maxWriteBytes)
    {
        super();
        this.networkUsageQuotaEnforcer = requireNonNull(networkUsageQuotaEnforcer, "networkUsageQuotaEnforcer is null");
        checkArgument(maxReadBytes >= 0, "The max read bytes cannot be negative");
        this.maxReadBytes = maxReadBytes;
        checkArgument(maxWriteBytes >= 0, "The max write bytes cannot be negative");
        this.maxWriteBytes = maxWriteBytes;
        this.networkUsageQuotaEnforcer.checkLimitsAndThrowIfExceeded(this.maxReadBytes, this.maxWriteBytes);
    }

    @Override
    public void recordReadBytes(long bytes)
    {
        super.recordReadBytes(bytes);
        networkUsageQuotaEnforcer.recordReadBytesAndThrowIfLimitExceeded(bytes, maxReadBytes);
    }

    @Override
    public void recordWriteBytes(long bytes)
    {
        super.recordWriteBytes(bytes);
        networkUsageQuotaEnforcer.recordWriteBytesAndThrowIfLimitExceeded(bytes, maxWriteBytes);
    }

    @FormatMethod
    private static void checkArgument(boolean argument, String format, Object... args)
    {
        if (!argument) {
            throw new IllegalArgumentException(format.formatted(args));
        }
    }
}
