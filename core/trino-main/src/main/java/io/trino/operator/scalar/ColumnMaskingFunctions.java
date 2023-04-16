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
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;

import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.airlift.slice.SliceUtf8.getCodePointAt;
import static io.airlift.slice.SliceUtf8.isAscii;
import static io.airlift.slice.SliceUtf8.substring;
import static io.trino.spi.type.StandardTypes.VARCHAR;
import static java.lang.Character.isWhitespace;

public class ColumnMaskingFunctions
{
    private static final byte STRING_MASKING_CHAR = 'x';
    private static final byte INTEGER_MASKING_CHAR = '1';
    private static final byte MINUS_SIGN = '-';
    private static final byte PLUS_SIGN = '+';

    private ColumnMaskingFunctions() {}

    @SqlType(VARCHAR)
    @ScalarFunction(value = "$sb_internal_mask_varchar", hidden = true)
    @Description("masks every non ASCII whitespace char in varchar with x")
    public static Slice maskVarchar(@SqlType(VARCHAR) Slice source)
    {
        int length = countCodePoints(source);
        if (length == 0) {
            return source;
        }
        byte[] result = new byte[length];
        maskedCopy(source, result, 0, STRING_MASKING_CHAR);
        return Slices.wrappedBuffer(result);
    }

    @SqlType(VARCHAR)
    @ScalarFunction(value = "$sb_internal_mask_integer_as_varchar", hidden = true)
    @Description("masks every digit in a string with 1")
    public static Slice maskIntegerAsVarchar(@SqlType(VARCHAR) Slice source)
    {
        int length = countCodePoints(source);
        if (length == 0) {
            return source;
        }
        byte[] result = new byte[length];
        switch (source.getByte(0)) {
            case MINUS_SIGN: {
                result[0] = MINUS_SIGN;
                break;
            }
            case PLUS_SIGN: {
                result[0] = PLUS_SIGN;
                break;
            }
            default:
                result[0] = INTEGER_MASKING_CHAR;
        }
        for (int i = 1; i < length; i++) {
            result[i] = INTEGER_MASKING_CHAR;
        }
        return Slices.wrappedBuffer(result);
    }

    @SqlType(VARCHAR)
    @ScalarFunction(value = "$sb_internal_mask_varchar_all_but_first_four", hidden = true)
    @Description("masks every non ASCII whitespace char in varchar with x except for first four characters")
    public static Slice maskAllButFirstFour(@SqlType(VARCHAR) Slice toBeMasked)
    {
        int numberOfCharactersToLeave = 4;
        int length = countCodePoints(toBeMasked);
        if (length == 0) {
            return toBeMasked;
        }
        if (length <= numberOfCharactersToLeave) {
            return toBeMasked;
        }

        Slice retain = substring(toBeMasked, 0, numberOfCharactersToLeave);
        byte[] result = new byte[retain.length() + length - numberOfCharactersToLeave];
        retain.getBytes(0, result, 0, retain.length());
        maskedCopy(toBeMasked.slice(retain.length(), toBeMasked.length() - retain.length()), result, retain.length(), STRING_MASKING_CHAR);
        return Slices.wrappedBuffer(result);
    }

    private static void maskedCopy(Slice source, byte[] result, int offset, byte mask)
    {
        int codePointsCount = countCodePoints(source);
        for (int i = 0; i < codePointsCount; i++) {
            Slice currentCodePoint = substring(source, i, 1);
            if (isAscii(currentCodePoint) && isWhitespace(getCodePointAt(currentCodePoint, 0))) {
                currentCodePoint.getBytes(0, result, offset + i, currentCodePoint.length());
            }
            else {
                result[offset + i] = mask;
            }
        }
    }

    @SqlType(VARCHAR)
    @ScalarFunction(value = "$sb_internal_mask_varchar_all_but_last_four", hidden = true)
    @Description("masks every non ASCII whitespace char in varchar with x except for first four characters")
    public static Slice maskAllButLastFour(@SqlType(VARCHAR) Slice toBeMasked)
    {
        int numberOfCharactersToLeave = 4;
        int length = countCodePoints(toBeMasked);
        if (length == 0) {
            return toBeMasked;
        }
        if (length <= numberOfCharactersToLeave) {
            return toBeMasked;
        }
        Slice retain = substring(toBeMasked, length - numberOfCharactersToLeave, numberOfCharactersToLeave);
        byte[] result = new byte[retain.length() + length - numberOfCharactersToLeave];
        retain.getBytes(0, result, result.length - retain.length(), retain.length());
        maskedCopy(toBeMasked.slice(0, toBeMasked.length() - retain.length()), result, 0, STRING_MASKING_CHAR);
        return Slices.wrappedBuffer(result);
    }
}
