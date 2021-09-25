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

import com.google.common.base.Splitter;
import com.google.common.collect.Range;
import com.google.common.net.InetAddresses;

import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Integer.parseInt;
import static java.util.Objects.requireNonNull;

public final class CidrBlock
{
    private static final Splitter CIDR_SPLITTER = Splitter.on('/').limit(2);

    private final String cidrBlock;
    private final Range<BigInteger> numericIpRange;

    public CidrBlock(String cidrBlock)
    {
        this.cidrBlock = requireNonNull(cidrBlock, "cidrBlock is null");
        this.numericIpRange = cidrToNumericIpRange(cidrBlock);
    }

    public boolean contains(InetAddress address)
    {
        return numericIpRange.contains(InetAddresses.toBigInteger(address));
    }

    @Override
    public String toString()
    {
        return cidrBlock;
    }

    private static Range<BigInteger> cidrToNumericIpRange(String cidr)
    {
        // TODO: we can make this more efficient if we don't use BigInteger
        List<String> parts = CIDR_SPLITTER.splitToList(cidr);
        InetAddress address = InetAddresses.forString(parts.get(0));
        int addressBits = addressBits(address);
        int maskBits = parts.size() == 2 ? parseInt(parts.get(1)) : addressBits;
        checkArgument(maskBits <= addressBits, "IP mask bits (%s) cannot be more than address bits (%s)", maskBits, addressBits);
        int hostBits = addressBits - maskBits;
        BigInteger fullMask = BigInteger.ONE.shiftLeft(addressBits).subtract(BigInteger.ONE);
        BigInteger hostMask = BigInteger.ONE.shiftLeft(hostBits).subtract(BigInteger.ONE);
        BigInteger subnetMask = fullMask.xor(hostMask);
        BigInteger cidrAddress = InetAddresses.toBigInteger(address);
        BigInteger minIpAddress = cidrAddress.and(subnetMask);
        BigInteger maxIpAddress = cidrAddress.or(hostMask);
        return Range.closed(minIpAddress, maxIpAddress);
    }

    private static int addressBits(InetAddress inetAddress)
    {
        if (inetAddress instanceof Inet4Address) {
            return 32;
        }
        if (inetAddress instanceof Inet6Address) {
            return 128;
        }
        throw new IllegalArgumentException("Unsupported InetAddress type: " + inetAddress.getClass());
    }
}
