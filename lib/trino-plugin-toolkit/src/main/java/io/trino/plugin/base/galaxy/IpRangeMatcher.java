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

import com.google.common.collect.ImmutableList;

import java.net.InetAddress;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class IpRangeMatcher
{
    private final List<CidrBlock> cidrBlocks;

    public IpRangeMatcher(List<CidrBlock> cidrBlocks)
    {
        this.cidrBlocks = ImmutableList.copyOf(requireNonNull(cidrBlocks, "cidrBlocks is null"));
    }

    public static IpRangeMatcher create(List<String> ips)
    {
        return new IpRangeMatcher(ips.stream()
                .map(CidrBlock::new)
                .collect(toImmutableList()));
    }

    public boolean matches(InetAddress inetAddress)
    {
        return cidrBlocks.stream().anyMatch(block -> block.contains(inetAddress));
    }
}
