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
package io.trino.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.execution.ScheduledSplit;
import io.trino.metadata.Split;
import io.trino.metadata.TableHandle;
import io.trino.operator.ValuesOperator.ValuesOperatorFactory;
import io.trino.spi.connector.ConnectorAlternativeChooser;
import io.trino.spi.connector.SchemaTableName;
import io.trino.split.AlternativeChooser;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.NullOutputOperator.NullOutputOperatorFactory;
import io.trino.testing.TestingMetadata.TestingTableHandle;
import io.trino.testing.TestingSplit;
import io.trino.testing.TestingTransactionHandle;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.TestingOperatorContext.createDriverContext;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestAlternativesAwareDriverFactory
{
    public static final PlanNodeId CHOOSE_ALTERNATIVE_NODE_ID = new PlanNodeId("chooseAlternative");
    private ScheduledExecutorService scheduledExecutor;

    @BeforeClass
    public void setUp()
    {
        scheduledExecutor = newSingleThreadScheduledExecutor();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testCorrectAlternativeDriversCreated()
    {
        AtomicInteger currentAlternative = new AtomicInteger(0);
        ConnectorAlternativeChooser connectorAlternativeChooser = (session, split, alternatives) ->
                new ConnectorAlternativeChooser.Choice(currentAlternative.get(), (transaction, session1, columns, dynamicFilter) -> {
                    throw new UnsupportedOperationException();
                });

        MockOperatorFactory alternativeOperatorFactory0 = new MockOperatorFactory();
        MockOperatorFactory alternativeOperatorFactory1 = new MockOperatorFactory();
        AlternativesAwareDriverFactory factory = new AlternativesAwareDriverFactory(
                new AlternativeChooser(catalogHandle -> connectorAlternativeChooser),
                TEST_SESSION,
                alternatives(ImmutableMap.of(
                        "alternative0", alternativeOperatorFactory0,
                        "alternative1", alternativeOperatorFactory1)),
                CHOOSE_ALTERNATIVE_NODE_ID,
                0,
                true,
                false,
                OptionalInt.empty());

        Driver driver0 = factory.createDriver(createDriverContext(scheduledExecutor), Optional.of(split(0)));
        assertEquals(alternativeOperatorFactory0.createdOperators, 1);
        assertTrue(driver0.getDriverContext().getConnectorAlternativePageSourceProvider().isPresent());
        assertEquals(driver0.getDriverContext().getAlternativeId(), Optional.of(0));

        currentAlternative.set(1);
        Driver driver1 = factory.createDriver(createDriverContext(scheduledExecutor), Optional.of(split(1)));
        assertEquals(alternativeOperatorFactory0.createdOperators, 1);
        assertTrue(driver1.getDriverContext().getConnectorAlternativePageSourceProvider().isPresent());
        assertEquals(driver1.getDriverContext().getAlternativeId(), Optional.of(1));

        currentAlternative.set(0);
        Driver driver2 = factory.createDriver(createDriverContext(scheduledExecutor), Optional.of(split(2)));
        assertEquals(alternativeOperatorFactory0.createdOperators, 2);
        assertTrue(driver2.getDriverContext().getConnectorAlternativePageSourceProvider().isPresent());
        assertEquals(driver2.getDriverContext().getAlternativeId(), Optional.of(0));
    }

    private static ScheduledSplit split(int sequenceId)
    {
        return new ScheduledSplit(sequenceId, CHOOSE_ALTERNATIVE_NODE_ID, new Split(TEST_CATALOG_HANDLE, TestingSplit.createLocalSplit()));
    }

    private static Map<TableHandle, DriverFactory> alternatives(Map<String, MockOperatorFactory> alternatives)
    {
        return alternatives.entrySet().stream().collect(toImmutableMap(
                entry -> new TableHandle(TEST_CATALOG_HANDLE, new TestingTableHandle(new SchemaTableName("test", entry.getKey())), TestingTransactionHandle.create()),
                entry -> new DriverFactory(
                        0,
                        true,
                        false,
                        ImmutableList.of(entry.getValue(), new NullOutputOperatorFactory(1, new PlanNodeId("out"))),
                        OptionalInt.empty())));
    }

    private static class MockOperatorFactory
            implements OperatorFactory
    {
        private final ValuesOperatorFactory delegate;
        private int createdOperators;

        MockOperatorFactory()
        {
            this(new ValuesOperatorFactory(0, new PlanNodeId("0"), ImmutableList.of()));
        }

        MockOperatorFactory(ValuesOperatorFactory delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            createdOperators++;
            return delegate.createOperator(driverContext);
        }

        @Override
        public void noMoreOperators()
        {
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new MockOperatorFactory(delegate);
        }
    }
}
