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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public class TestThreadCountParser
{
    private static final int AVAILABLE_PROCESSORS = 2;

    @Test
    public void testParsingIntegerValues()
    {
        assertThreadsCount("1", 1);
        assertThreadsCount("2", 2);
        assertThreadsCount("67", 67);
        assertThreadsCount("0", 0);
        assertThreadsCount(Integer.valueOf(Integer.MAX_VALUE).toString(), Integer.MAX_VALUE);
        assertInvalidValue("-1", "Thread count cannot be negative");
        assertInvalidValue("67.0", "For input string: \"67.0\"");
        assertInvalidValue(Long.valueOf(((long) Integer.MAX_VALUE) + 1).toString(), "Thread count is greater than 2^32 - 1");
    }

    @Test
    public void testParsingMultiplierPerCore()
    {
        assertThreadsCount("1C", AVAILABLE_PROCESSORS);
        assertThreadsCount("2 C", AVAILABLE_PROCESSORS * 2);
        assertInvalidValue("-1C", "Thread multiplier cannot be negative");
        assertInvalidValue("-1AC", "For input string: \"-1A\"");
        assertInvalidValue("2147483647C", "Thread count is greater than 2^32 - 1");
        assertInvalidValue("3147483648C", "Thread count is greater than 2^32 - 1");
    }

    private void assertThreadsCount(String value, int expected)
    {
        int threadCount = new ThreadCountParser(() -> AVAILABLE_PROCESSORS).parse(value);
        assertThat(threadCount).isEqualTo(expected);
    }

    private void assertInvalidValue(String value, String expectedMessage)
    {
        ThreadCountParser parser = new ThreadCountParser(() -> AVAILABLE_PROCESSORS);
        assertThatThrownBy(() -> parser.parse(value))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(expectedMessage);
    }
}
