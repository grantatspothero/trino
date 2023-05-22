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
package io.trino.parquet.reader.decoders;

import io.trino.parquet.reader.SimpleSliceInputStream;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorShuffle;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.parquet.ParquetReaderUtils.propagateSignBit;

public class ShortDecimalFixedWidthByteArrayBatchDecoder
{
    private static final ShortDecimalDecoder[] VALUE_DECODERS = new ShortDecimalDecoder[] {
            new BigEndianReader1(),
            new BigEndianReader2(),
            new BigEndianReader3(),
            new BigEndianReader4(),
            new BigEndianReader5(),
            new BigEndianReader6(),
            new BigEndianReader7(),
            new BigEndianReader8()
    };

    public interface ShortDecimalDecoder
    {
        void decode(SimpleSliceInputStream input, long[] values, int offset, int length);

        void vectorDecode(SimpleSliceInputStream input, long[] values, int offset, int length);
    }

    private final ShortDecimalDecoder decoder;
    private final boolean vectorizedDecodingEnabled;

    public ShortDecimalFixedWidthByteArrayBatchDecoder(int length, boolean vectorizedDecodingEnabled)
    {
        checkArgument(
                length > 0 && length <= 8,
                "Short decimal length %s must be in range 1-8",
                length);
        decoder = VALUE_DECODERS[length - 1];
        this.vectorizedDecodingEnabled = vectorizedDecodingEnabled;
        // Unscaled number is encoded as two's complement using big-endian byte order
        // (the most significant byte is the zeroth element)
    }

    /**
     * This method uses Unsafe operations on Slice.
     * Always check if needed data is available with ensureBytesAvailable method.
     * Failing to do so may result in instant JVM crash.
     */
    public void getShortDecimalValues(SimpleSliceInputStream input, long[] values, int offset, int length)
    {
        if (vectorizedDecodingEnabled) {
            decoder.vectorDecode(input, values, offset, length);
        }
        else {
            decoder.decode(input, values, offset, length);
        }
    }

    private static final class BigEndianReader8
            implements ShortDecimalDecoder
    {
        private static final VectorShuffle<Byte> SHUFFLE = VectorShuffle.fromArray(
                ByteVector.SPECIES_128,
                new int[] {
                        7, 6, 5, 4, 3, 2, 1, 0,
                        15, 14, 13, 12, 11, 10, 9, 8
                },
                0);

        @Override
        public void decode(SimpleSliceInputStream input, long[] values, int offset, int length)
        {
            int endOffset = offset + length;
            for (int i = offset; i < endOffset; i++) {
                values[i] = Long.reverseBytes(input.readLongUnsafe());
            }
        }

        @Override
        public void vectorDecode(SimpleSliceInputStream input, long[] values, int offset, int length)
        {
            byte[] inputArr = input.getByteArray();
            int inputOffset = input.getByteArrayOffset();
            int inputBytesRead = 0;
            int outputOffset = offset;
            while (length > 1) {
                ByteVector.fromArray(ByteVector.SPECIES_128, inputArr, inputOffset + inputBytesRead)
                        .rearrange(SHUFFLE)
                        .reinterpretAsLongs()
                        .intoArray(values, outputOffset);
                inputBytesRead += 16;
                length -= 2;
                outputOffset += 2;
            }
            input.skip(inputBytesRead);

            // Decode the last value "normally" as it would read data out of bounds
            if (length > 0) {
                values[outputOffset] = Long.reverseBytes(input.readLongUnsafe());
            }
        }
    }

    private static final class BigEndianReader7
            implements ShortDecimalDecoder
    {
        private static final VectorShuffle<Byte> SHUFFLE = VectorShuffle.fromArray(
                ByteVector.SPECIES_256,
                new int[] {
                        7, 6, 5, 4, 3, 2, 1, 0,
                        14, 13, 12, 11, 10, 9, 8, 7,
                        21, 20, 19, 18, 17, 16, 15, 14,
                        28, 27, 26, 25, 24, 23, 22, 21
                },
                0);

        @Override
        public void decode(SimpleSliceInputStream input, long[] values, int offset, int length)
        {
            int bytesOffSet = 0;
            int endOffset = offset + length;
            for (int i = offset; i < endOffset - 1; i++) {
                // We read redundant bytes and then ignore them. Sign bit is propagated by `>>` operator
                values[i] = Long.reverseBytes(input.getLongUnsafe(bytesOffSet)) >> 8;
                bytesOffSet += 7;
            }
            // Decode the last one "normally" as it would read data out of bounds
            values[endOffset - 1] = decode(input, bytesOffSet);
            input.skip(bytesOffSet + 7);
        }

        @Override
        public void vectorDecode(SimpleSliceInputStream input, long[] values, int offset, int length)
        {
            byte[] inputArr = input.getByteArray();
            int inputOffset = input.getByteArrayOffset();
            int inputBytesRead = 0;
            int outputOffset = offset;
            while (length > 4) {
                // We read redundant bytes and then ignore them. Sign bit is propagated by `>>` operator
                ByteVector.fromArray(ByteVector.SPECIES_256, inputArr, inputOffset + inputBytesRead)
                        .rearrange(SHUFFLE)
                        .reinterpretAsLongs()
                        .lanewise(VectorOperators.ASHR, 8)
                        .intoArray(values, outputOffset);
                inputBytesRead += 28;
                length -= 4;
                outputOffset += 4;
            }
            // Decode the last 5 values "normally" as it would read data out of bounds
            while (length > 0) {
                values[outputOffset++] = decode(input, inputBytesRead);
                inputBytesRead += 7;
                length--;
            }
            input.skip(inputBytesRead);
        }

        private long decode(SimpleSliceInputStream input, int index)
        {
            long value = (input.getByteUnsafe(index + 6) & 0xFFL)
                    | (input.getByteUnsafe(index + 5) & 0xFFL) << 8
                    | (input.getByteUnsafe(index + 4) & 0xFFL) << 16
                    | (Integer.reverseBytes(input.getIntUnsafe(index)) & 0xFFFFFFFFL) << 24;
            return propagateSignBit(value, 8);
        }
    }

    private static final class BigEndianReader6
            implements ShortDecimalDecoder
    {
        private static final VectorShuffle<Byte> SHUFFLE = VectorShuffle.fromArray(
                ByteVector.SPECIES_256,
                new int[] {
                        7, 6, 5, 4, 3, 2, 1, 0,
                        13, 12, 11, 10, 9, 8, 7, 6,
                        19, 18, 17, 16, 15, 14, 13, 12,
                        25, 24, 23, 22, 21, 20, 19, 18
                },
                0);

        @Override
        public void decode(SimpleSliceInputStream input, long[] values, int offset, int length)
        {
            int bytesOffSet = 0;
            int endOffset = offset + length;
            for (int i = offset; i < endOffset - 1; i++) {
                // We read redundant bytes and then ignore them. Sign bit is propagated by `>>` operator
                values[i] = Long.reverseBytes(input.getLongUnsafe(bytesOffSet)) >> 16;
                bytesOffSet += 6;
            }
            // Decode the last one "normally" as it would read data out of bounds
            values[endOffset - 1] = decode(input, bytesOffSet);
            input.skip(bytesOffSet + 6);
        }

        @Override
        public void vectorDecode(SimpleSliceInputStream input, long[] values, int offset, int length)
        {
            byte[] inputArr = input.getByteArray();
            int inputOffset = input.getByteArrayOffset();
            int inputBytesRead = 0;
            int outputOffset = offset;
            while (length > 5) {
                // We read redundant bytes and then ignore them. Sign bit is propagated by `>>` operator
                ByteVector.fromArray(ByteVector.SPECIES_256, inputArr, inputOffset + inputBytesRead)
                        .rearrange(SHUFFLE)
                        .reinterpretAsLongs()
                        .lanewise(VectorOperators.ASHR, 16)
                        .intoArray(values, outputOffset);
                inputBytesRead += 24;
                length -= 4;
                outputOffset += 4;
            }
            // Decode the last 5 values "normally" as it would read data out of bounds
            while (length > 0) {
                values[outputOffset++] = decode(input, inputBytesRead);
                inputBytesRead += 6;
                length--;
            }
            input.skip(inputBytesRead);
        }

        private long decode(SimpleSliceInputStream input, int index)
        {
            long value = (input.getByteUnsafe(index + 5) & 0xFFL)
                    | (input.getByteUnsafe(index + 4) & 0xFFL) << 8
                    | (Integer.reverseBytes(input.getIntUnsafe(index)) & 0xFFFFFFFFL) << 16;
            return propagateSignBit(value, 16);
        }
    }

    private static final class BigEndianReader5
            implements ShortDecimalDecoder
    {
        private static final VectorShuffle<Byte> SHUFFLE = VectorShuffle.fromArray(
                ByteVector.SPECIES_256,
                new int[] {
                        7, 6, 5, 4, 3, 2, 1, 0,
                        12, 11, 10, 9, 8, 7, 6, 5,
                        17, 16, 15, 14, 13, 12, 11, 10,
                        22, 21, 20, 19, 18, 17, 16, 15
                },
                0);

        @Override
        public void decode(SimpleSliceInputStream input, long[] values, int offset, int length)
        {
            int bytesOffSet = 0;
            int endOffset = offset + length;
            for (int i = offset; i < endOffset - 1; i++) {
                // We read redundant bytes and then ignore them. Sign bit is propagated by `>>` operator
                values[i] = Long.reverseBytes(input.getLongUnsafe(bytesOffSet)) >> 24;
                bytesOffSet += 5;
            }
            // Decode the last one "normally" as it would read data out of bounds
            values[endOffset - 1] = decode(input, bytesOffSet);
            input.skip(bytesOffSet + 5);
        }

        @Override
        public void vectorDecode(SimpleSliceInputStream input, long[] values, int offset, int length)
        {
            byte[] inputArr = input.getByteArray();
            int inputOffset = input.getByteArrayOffset();
            int inputBytesRead = 0;
            int outputOffset = offset;
            while (length > 6) {
                // We read redundant bytes and then ignore them. Sign bit is propagated by `>>` operator
                ByteVector.fromArray(ByteVector.SPECIES_256, inputArr, inputOffset + inputBytesRead)
                        .rearrange(SHUFFLE)
                        .reinterpretAsLongs()
                        .lanewise(VectorOperators.ASHR, 24)
                        .intoArray(values, outputOffset);
                inputBytesRead += 20;
                length -= 4;
                outputOffset += 4;
            }
            // Decode the last 7 values "normally" as it would read data out of bounds
            while (length > 0) {
                values[outputOffset++] = decode(input, inputBytesRead);
                inputBytesRead += 5;
                length--;
            }
            input.skip(inputBytesRead);
        }

        private long decode(SimpleSliceInputStream input, int index)
        {
            long value = (input.getByteUnsafe(index + 4) & 0xFFL)
                    | (Integer.reverseBytes(input.getIntUnsafe(index)) & 0xFFFFFFFFL) << 8;
            return propagateSignBit(value, 24);
        }
    }

    private static final class BigEndianReader4
            implements ShortDecimalDecoder
    {
        private static final VectorShuffle<Byte> SHUFFLE = VectorShuffle.fromArray(
                ByteVector.SPECIES_128,
                new int[] {
                        3, 2, 1, 0, 7, 6, 5, 4,
                        11, 10, 9, 8, 15, 14, 13, 12
                },
                0);

        @Override
        public void decode(SimpleSliceInputStream input, long[] values, int offset, int length)
        {
            while (length > 1) {
                long value = Long.reverseBytes(input.readLongUnsafe());

                // Implicit cast will propagate the sign bit correctly, as it is performed after the byte reversal.
                values[offset] = (int) (value >> 32);
                values[offset + 1] = (int) value;

                offset += 2;
                length -= 2;
            }

            if (length > 0) {
                int value = input.readIntUnsafe();
                values[offset] = Integer.reverseBytes(value);
            }
        }

        @Override
        public void vectorDecode(SimpleSliceInputStream input, long[] values, int offset, int length)
        {
            byte[] inputArr = input.getByteArray();
            int inputOffset = input.getByteArrayOffset();
            int inputBytesRead = 0;
            int outputOffset = offset;
            while (length > 3) {
                ByteVector.fromArray(ByteVector.SPECIES_128, inputArr, inputOffset + inputBytesRead)
                        .rearrange(SHUFFLE)
                        .reinterpretAsInts()
                        .castShape(LongVector.SPECIES_256, 0)
                        .reinterpretAsLongs()
                        .intoArray(values, outputOffset);
                inputBytesRead += 16;
                outputOffset += 4;
                length -= 4;
            }
            input.skip(inputBytesRead);

            while (length > 0) {
                values[outputOffset++] = Integer.reverseBytes(input.readIntUnsafe());
                length--;
            }
        }
    }

    private static final class BigEndianReader3
            implements ShortDecimalDecoder
    {
        private static final VectorShuffle<Byte> SHUFFLE = VectorShuffle.fromArray(
                ByteVector.SPECIES_128,
                new int[] {
                        3, 2, 1, 0, 6, 5, 4, 3,
                        9, 8, 7, 6, 12, 11, 10, 9
                },
                0);

        @Override
        public void decode(SimpleSliceInputStream input, long[] values, int offset, int length)
        {
            int bytesOffSet = 0;
            int endOffset = offset + length;
            int i = offset;
            for (; i < endOffset - 2; i += 2) {
                // We read redundant bytes and then ignore them. Sign bit is propagated by `>>` operator
                long value = Long.reverseBytes(input.getLongUnsafe(bytesOffSet));
                values[i] = value >> 40;
                values[i + 1] = value << 24 >> 40;
                bytesOffSet += 6;
            }
            // Decode the last values "normally" as it would read data out of bounds
            while (i < endOffset) {
                values[i++] = decode(input, bytesOffSet);
                bytesOffSet += 3;
            }
            input.skip(bytesOffSet);
        }

        @Override
        public void vectorDecode(SimpleSliceInputStream input, long[] values, int offset, int length)
        {
            byte[] inputArr = input.getByteArray();
            int inputOffset = input.getByteArrayOffset();
            int inputBytesRead = 0;
            int outputOffset = offset;
            while (length > 5) {
                ByteVector.fromArray(ByteVector.SPECIES_128, inputArr, inputOffset + inputBytesRead)
                        .rearrange(SHUFFLE)
                        .reinterpretAsInts()
                        .lanewise(VectorOperators.ASHR, 8)
                        .castShape(LongVector.SPECIES_256, 0)
                        .reinterpretAsLongs()
                        .intoArray(values, outputOffset);
                inputBytesRead += 12;
                outputOffset += 4;
                length -= 4;
            }

            while (length > 0) {
                values[outputOffset++] = decode(input, inputBytesRead);
                inputBytesRead += 3;
                length--;
            }
            input.skip(inputBytesRead);
        }

        private long decode(SimpleSliceInputStream input, int index)
        {
            long value = (input.getByteUnsafe(index + 2) & 0xFFL)
                    | (input.getByteUnsafe(index + 1) & 0xFFL) << 8
                    | (input.getByteUnsafe(index) & 0xFFL) << 16;
            return propagateSignBit(value, 40);
        }
    }

    private static final class BigEndianReader2
            implements ShortDecimalDecoder
    {
        private static final VectorShuffle<Byte> SHUFFLE = VectorShuffle.fromArray(
                ByteVector.SPECIES_64,
                new int[] {1, 0, 3, 2, 5, 4, 7, 6},
                0);

        @Override
        public void decode(SimpleSliceInputStream input, long[] values, int offset, int length)
        {
            while (length > 3) {
                long value = input.readLongUnsafe();
                // Reverse all bytes at once
                value = Long.reverseBytes(value);

                // We first shift the byte as left as possible. Then, when shifting back right,
                // the sign bit will get propagated
                values[offset] = value >> 48;
                values[offset + 1] = value << 16 >> 48;
                values[offset + 2] = value << 32 >> 48;
                values[offset + 3] = value << 48 >> 48;

                offset += 4;
                length -= 4;
            }

            while (length > 0) {
                // Implicit cast will propagate the sign bit correctly, as it is performed after the byte reversal.
                values[offset++] = Short.reverseBytes(input.readShort());
                length--;
            }
        }

        @Override
        public void vectorDecode(SimpleSliceInputStream input, long[] values, int offset, int length)
        {
            byte[] inputArr = input.getByteArray();
            int inputOffset = input.getByteArrayOffset();
            int inputBytesRead = 0;
            int outputOffset = offset;
            while (length > 3) {
                ByteVector.fromArray(ByteVector.SPECIES_64, inputArr, inputOffset + inputBytesRead)
                        .rearrange(SHUFFLE)
                        .reinterpretAsShorts()
                        .castShape(LongVector.SPECIES_256, 0)
                        .reinterpretAsLongs()
                        .intoArray(values, outputOffset);
                inputBytesRead += 8;
                outputOffset += 4;
                length -= 4;
            }
            input.skip(inputBytesRead);

            while (length > 0) {
                // Implicit cast will propagate the sign bit correctly, as it is performed after the byte reversal.
                values[outputOffset++] = Short.reverseBytes(input.readShort());
                length--;
            }
        }
    }

    private static final class BigEndianReader1
            implements ShortDecimalDecoder
    {
        @Override
        public void decode(SimpleSliceInputStream input, long[] values, int offset, int length)
        {
            byte[] inputArr = input.getByteArray();
            int inputOffset = input.getByteArrayOffset();
            int inputBytesRead = 0;
            int outputOffset = offset;
            while (length > 0) {
                // Implicit cast will propagate the sign bit correctly
                values[outputOffset++] = inputArr[inputOffset + inputBytesRead];
                inputBytesRead++;
                length--;
            }
            input.skip(inputBytesRead);
        }

        @Override
        public void vectorDecode(SimpleSliceInputStream input, long[] values, int offset, int length)
        {
            decode(input, values, offset, length);
        }
    }
}
