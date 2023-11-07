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
package io.trino.cost;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

import static io.trino.SystemSessionProperties.HISTORY_BASED_STATISTICS_ENABLED;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public abstract class BaseStatsCalculatorTest
{
    private StatsCalculatorTester tester;

    @BeforeAll
    public void setUp()
    {
        tester = new StatsCalculatorTester(testSessionBuilder().setSystemProperty(HISTORY_BASED_STATISTICS_ENABLED, "false").build());
    }

    @AfterAll
    public void tearDown()
    {
        tester.close();
        tester = null;
    }

    protected StatsCalculatorTester tester()
    {
        return tester;
    }
}
