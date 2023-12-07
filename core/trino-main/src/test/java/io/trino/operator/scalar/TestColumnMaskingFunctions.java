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
package io.trino.operator.scalar;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static io.trino.operator.scalar.ColumnMaskingFunctions.maskAllButFirstFour;
import static io.trino.operator.scalar.ColumnMaskingFunctions.maskAllButLastFour;
import static io.trino.operator.scalar.ColumnMaskingFunctions.maskIntegerAsVarchar;
import static io.trino.operator.scalar.ColumnMaskingFunctions.maskVarchar;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestColumnMaskingFunctions
{
    public Object[][] differentStringsWithFullMask()
    {
        return new Object[][] {
                {"", ""},
                {"\t \n  \u000B \f \n \u0085 \u1680 \u2000 \u2001 \u2002 \u2003 \u2004 \u2005 \u2006 \u2007 \u2008 \u2009 \u200A \u2028 \u2029 \u202F \u205F \u3000",
                        "\t \n  \u000B \f \n x x x x x x x x x x x x x x x x x x"},
                {"Sed ut perspiciatis", "xxx xx xxxxxxxxxxxx"},
                {"This is a bear \ud83d\udc3b", "xxxx xx x xxxx x"},
                {"\ud83d\udc3b a bear this is", "x x xxxx xxxx xx"},
                {"\uc2ae\uc2b1\uc386\uc398\uc3a6\uc3b7\uc3be\uc2b5", "xxxxxxxx"},
                {"Iñtërnâtiônàlizætiøn", "xxxxxxxxxxxxxxxxxxxx"},
                {"\uD83D\uDC9D\uD83D\uDC39\uD83C\uDF07\u26d4", "xxxx"},
                {"\uD83D\uDC9D\uD83D\uDC39\uD83C\uDF07 a heart a hamster a city", "xxx x xxxxx x xxxxxxx x xxxx"},
                {"\uD83D\uDC9D\n\uD83D\uDC39\n\uD83C\uDF07\n a heart\n a hamster\n a city", "x\nx\nx\n x xxxxx\n x xxxxxxx\n x xxxx"},
        };
    }

    public Object[][] differentStringsWithAllButFirstFour()
    {
        return new Object[][] {
                {"", ""},
                {"\t \n  \u000B \f \n \u0085 \u1680 \u2000 \u2001 \u2002 \u2003 \u2004 \u2005 \u2006 \u2007 \u2008 \u2009 \u200A \u2028 \u2029 \u202F \u205F \u3000",
                        "\t \n  \u000B \f \n x x x x x x x x x x x x x x x x x x"},
                {"Sed ut perspiciatis", "Sed xx xxxxxxxxxxxx"},
                {"This is a bear \ud83d\udc3b", "This xx x xxxx x"},
                {"\ud83d\udc3b a bear this is", "\ud83d\udc3b a xxxx xxxx xx"},
                {"\uc2ae\uc2b1\uc386\uc398\uc3a6\uc3b7\uc3be\uc2b5", "\uc2ae\uc2b1\uc386\uc398xxxx"},
                {"Iñtërnâtiônàlizætiøn", "Iñtëxxxxxxxxxxxxxxxx"},
                {"\uD83D\uDC9D\uD83D\uDC39\uD83C\uDF07\u26d4", "\uD83D\uDC9D\uD83D\uDC39\uD83C\uDF07\u26d4"},
                {"\uD83D\uDC9D\uD83D\uDC39\uD83C\uDF07 a heart a hamster a city", "\uD83D\uDC9D\uD83D\uDC39\uD83C\uDF07 x xxxxx x xxxxxxx x xxxx"},
                {"\uD83D\uDC9D\n\uD83D\uDC39\n\uD83C\uDF07\n a heart\n a hamster\n a city", "\uD83D\uDC9D\n\uD83D\uDC39\nx\n x xxxxx\n x xxxxxxx\n x xxxx"},
        };
    }

    public Object[][] differentStringsWithAllButLastFour()
    {
        return new Object[][] {
                {"", ""},
                {"\t \n  \u000B \f \n \u0085 \u1680 \u2000 \u2001 \u2002 \u2003 \u2004 \u2005 \u2006 \u2007 \u2008 \u2009 \u200A \u2028 \u2029 \u202F \u205F \u3000",
                        "\t \n  \u000B \f \n x x x x x x x x x x x x x x x x \u205F \u3000"},
                {"Sed ut perspiciatis", "xxx xx xxxxxxxxatis"},
                {"This is a bear \ud83d\udc3b", "xxxx xx x xxar \ud83d\udc3b"},
                {"\ud83d\udc3b a bear this is", "x x xxxx xxxs is"},
                {"\uc2ae\uc2b1\uc386\uc398\uc3a6\uc3b7\uc3be\uc2b5", "xxxx\uc3a6\uc3b7\uc3be\uc2b5"},
                {"Iñtërnâtiônàlizætiøn", "xxxxxxxxxxxxxxxxtiøn"},
                {"Iñtërnâtiônàlizætiøn", "xxxxxxxxxxxxxxxxtiøn"},
                {"\uD83D\uDC9D\uD83D\uDC39\uD83C\uDF07\u26d4", "\uD83D\uDC9D\uD83D\uDC39\uD83C\uDF07\u26d4"},
                {"a heart a hamster a city \uD83D\uDC9D\uD83D\uDC39\uD83C\uDF07", "x xxxxx x xxxxxxx x xxxx \uD83D\uDC9D\uD83D\uDC39\uD83C\uDF07"},
                {"a heart\n a hamster\n a city\n \uD83D\uDC9D\n\uD83D\uDC39\n\uD83C\uDF07", "x xxxxx\n x xxxxxxx\n x xxxx\n x\n\uD83D\uDC39\n\uD83C\uDF07"},
        };
    }

    @Test
    public void testValidInvocations()
    {
        try (QueryAssertions assertions = new QueryAssertions()) {
            assertThat(assertions.function("$sb_internal_mask_varchar", "'fuu bar'"))
                    .hasType(createUnboundedVarcharType())
                    .isEqualTo("xxx xxx");
            assertThat(assertions.function("$sb_internal_mask_integer_as_varchar", "'121252'"))
                    .hasType(createUnboundedVarcharType())
                    .isEqualTo("111111");
            assertThat(assertions.function("$sb_internal_mask_varchar_all_but_first_four", "'fuu bar'"))
                    .hasType(createUnboundedVarcharType())
                    .isEqualTo("fuu xxx");
            assertThat(assertions.function("$sb_internal_mask_varchar_all_but_last_four", "'fuu bar'"))
                    .hasType(createUnboundedVarcharType())
                    .isEqualTo("xxx bar");
        }
    }

    @ParameterizedTest
    @MethodSource("differentStringsWithFullMask")
    public void testMaskVarchar(String data, String masked)
    {
        Slice toBeMasked = Slices.utf8Slice(data);

        Slice slice = maskVarchar(toBeMasked);

        assertThat(slice.toStringUtf8())
                .isEqualTo(masked);
    }

    @ParameterizedTest
    @MethodSource("differentStringsWithAllButFirstFour")
    public void testMaskAllButFirstFour(String data, String masked)
    {
        Slice toBeMasked = Slices.utf8Slice(data);

        Slice slice = maskAllButFirstFour(toBeMasked);

        assertThat(slice.toStringUtf8()).isEqualTo(masked);
    }

    @ParameterizedTest
    @MethodSource("differentStringsWithAllButLastFour")
    public void testMaskAllButLastFour(String data, String masked)
    {
        Slice toBeMasked = Slices.utf8Slice(data);

        Slice slice = maskAllButLastFour(toBeMasked);

        assertThat(slice.toStringUtf8()).isEqualTo(masked);
    }

    public Object[][] differentIntegers()
    {
        return new Object[][] {
                {""},
                {"1"},
                {"-1111"},
                {"+11111"},
                {"121121241251212312512411212"},
                {"999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999"},
        };
    }

    @ParameterizedTest
    @MethodSource("differentIntegers")
    public void testMaskIntegerAsVarchar(String integer)
    {
        Slice slice = Slices.utf8Slice(integer);
        assertThat(maskIntegerAsVarchar(slice).toStringUtf8())
                .isEqualTo(integer.replaceAll("\\d", "1"));
    }
}
