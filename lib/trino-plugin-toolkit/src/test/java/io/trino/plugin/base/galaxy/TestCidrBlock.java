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

import com.google.common.net.InetAddresses;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestCidrBlock
{
    @Test
    public void testV4()
    {
        assertTrue(new CidrBlock("1.2.3.4").contains(InetAddresses.forString("1.2.3.4")));
        assertFalse(new CidrBlock("1.2.3.4").contains(InetAddresses.forString("1.2.3.5")));

        assertTrue(new CidrBlock("1.2.3.4/32").contains(InetAddresses.forString("1.2.3.4")));
        assertFalse(new CidrBlock("1.2.3.4/32").contains(InetAddresses.forString("1.2.3.5")));

        assertTrue(new CidrBlock("1.2.3.4/31").contains(InetAddresses.forString("1.2.3.4")));
        assertTrue(new CidrBlock("1.2.3.4/31").contains(InetAddresses.forString("1.2.3.5")));
        assertFalse(new CidrBlock("1.2.3.4/31").contains(InetAddresses.forString("1.2.3.3")));
        assertFalse(new CidrBlock("1.2.3.4/31").contains(InetAddresses.forString("1.2.3.6")));
    }

    @Test
    public void testV6()
    {
        assertTrue(new CidrBlock("1122:3344:5566:7788:99aa:bbcc:ddee:ff00").contains(InetAddresses.forString("1122:3344:5566:7788:99aa:bbcc:ddee:ff00")));
        assertFalse(new CidrBlock("1122:3344:5566:7788:99aa:bbcc:ddee:ff00").contains(InetAddresses.forString("1122:3344:5566:7788:99aa:bbcc:ddee:ff01")));

        assertTrue(new CidrBlock("1122:3344:5566:7788:99aa:bbcc:ddee:ff00/128").contains(InetAddresses.forString("1122:3344:5566:7788:99aa:bbcc:ddee:ff00")));
        assertFalse(new CidrBlock("1122:3344:5566:7788:99aa:bbcc:ddee:ff00/128").contains(InetAddresses.forString("1122:3344:5566:7788:99aa:bbcc:ddee:ff01")));

        assertTrue(new CidrBlock("1122:3344:5566:7788:99aa:bbcc:ddee:ff10/127").contains(InetAddresses.forString("1122:3344:5566:7788:99aa:bbcc:ddee:ff10")));
        assertTrue(new CidrBlock("1122:3344:5566:7788:99aa:bbcc:ddee:ff10/127").contains(InetAddresses.forString("1122:3344:5566:7788:99aa:bbcc:ddee:ff11")));
        assertFalse(new CidrBlock("1122:3344:5566:7788:99aa:bbcc:ddee:ff10/127").contains(InetAddresses.forString("1122:3344:5566:7788:99aa:bbcc:ddee:ff0f")));
        assertFalse(new CidrBlock("1122:3344:5566:7788:99aa:bbcc:ddee:ff10/127").contains(InetAddresses.forString("1122:3344:5566:7788:99aa:bbcc:ddee:ff12")));

        assertTrue(new CidrBlock("1122:3344:5566:7788::/64").contains(InetAddresses.forString("1122:3344:5566:7788:99aa:bbcc:ddee:ff10")));
        assertTrue(new CidrBlock("1122:3344:5566:7788::/64").contains(InetAddresses.forString("1122:3344:5566:7788::FF")));
        assertFalse(new CidrBlock("1122:3344:5566:7788::/64").contains(InetAddresses.forString("1122:3344:5566:7789::0")));
    }

    @Test
    public void matchAll()
    {
        assertTrue(new CidrBlock("0.0.0.0/0").contains(InetAddresses.forString("1.2.3.6")));
        assertTrue(new CidrBlock("::/0").contains(InetAddresses.forString("0::1")));
        assertTrue(new CidrBlock("::/0").contains(InetAddresses.forString("11::88:99")));
    }
}
