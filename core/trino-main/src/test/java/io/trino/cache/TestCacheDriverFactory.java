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
package io.trino.cache;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.execution.ScheduledSplit;
import io.trino.memory.LocalMemoryManager;
import io.trino.memory.NodeMemoryConfig;
import io.trino.metadata.Split;
import io.trino.metadata.TableHandle;
import io.trino.operator.DevNullOperator;
import io.trino.operator.Driver;
import io.trino.operator.DriverContext;
import io.trino.operator.DriverFactory;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheSplitId;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.cache.SignatureKey;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.connector.TestingColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.split.PageSourceProvider;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.testing.TestingSplit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.cache.StaticDynamicFilter.createStaticDynamicFilter;
import static io.trino.spi.connector.DynamicFilter.EMPTY;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.TestingHandles.TEST_TABLE_HANDLE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestCacheDriverFactory
{
    private static final Session TEST_SESSION = testSessionBuilder().build();
    private final PlanNodeIdAllocator planNodeIdAllocator = new PlanNodeIdAllocator();
    private CacheManagerRegistry registry;

    @BeforeMethod
    public void setUp()
    {
        NodeMemoryConfig config = new NodeMemoryConfig()
                .setHeapHeadroom(DataSize.of(16, MEGABYTE))
                .setMaxQueryMemoryPerNode(DataSize.of(32, MEGABYTE));
        CacheConfig cacheConfig = new CacheConfig();
        cacheConfig.setEnabled(true);
        registry = new CacheManagerRegistry(cacheConfig, new LocalMemoryManager(config, DataSize.of(1024, MEGABYTE).toBytes()));
        registry.loadCacheManager();
    }

    @Test
    public void testUpdateCache()
    {
        PlanSignature signature = new PlanSignature(new SignatureKey("sig"), Optional.empty(), ImmutableList.of(), TupleDomain.all(), TupleDomain.all());
        AtomicInteger operatorIdAllocator = new AtomicInteger();

        DriverFactory driverFactory = new DriverFactory(
                operatorIdAllocator.incrementAndGet(),
                true,
                false,
                ImmutableList.of(
                        new DevNullOperator.DevNullOperatorFactory(0, planNodeIdAllocator.getNextId()),
                        new DevNullOperator.DevNullOperatorFactory(1, planNodeIdAllocator.getNextId())),
                OptionalInt.empty());

        CacheDriverFactory cacheDriverFactory = new CacheDriverFactory(
                TEST_SESSION,
                new TestPageSourceProvider(),
                registry,
                TEST_TABLE_HANDLE,
                signature,
                ImmutableMap.of(),
                () -> createStaticDynamicFilter(EMPTY),
                ImmutableList.of(driverFactory, driverFactory));

        PlanSignature planSignature = new PlanSignature(new SignatureKey("signature"), Optional.empty(), ImmutableList.of(), TupleDomain.all(), TupleDomain.all());
        PlanSignature planSignature2 = planSignature.withDynamicPredicate(TupleDomain.none());
        // it is the first time when splitCache is updated in driver factory - expect that it was set in factory

        cacheDriverFactory.updateSplitCache(planSignature, false);
        assertThat(cacheDriverFactory.getCachePlanSignature()).isEqualTo(planSignature);
        // dynamic filter was not changed - do not update it in factory
        cacheDriverFactory.updateSplitCache(planSignature.withDynamicPredicate(TupleDomain.all()), false);
        assertThat(cacheDriverFactory.getCachePlanSignature()).isEqualTo(planSignature);
        // dynamic filter was changed - do update it in factory
        cacheDriverFactory.updateSplitCache(planSignature2, true);
        assertThat(cacheDriverFactory.getCachePlanSignature()).isEqualTo(planSignature2);
        // dynamic filter was changed but planSignature was marked as final previously - do not update it in factory
        cacheDriverFactory.updateSplitCache(planSignature, false);
        assertThat(cacheDriverFactory.getCachePlanSignature()).isEqualTo(planSignature2);
    }

    @Test
    public void testCreateDriverForOriginalPlan()
    {
        PlanSignature signature = new PlanSignature(new SignatureKey("sig"), Optional.empty(), ImmutableList.of(), TupleDomain.all(), TupleDomain.all());
        AtomicInteger operatorIdAllocator = new AtomicInteger();
        ConnectorSplit connectorSplit = new TestingSplit(false, ImmutableList.of());
        Split split = new Split(TEST_CATALOG_HANDLE, connectorSplit);

        DriverFactory driverFactory = new DriverFactory(
                operatorIdAllocator.incrementAndGet(),
                true,
                false,
                ImmutableList.of(
                        new DevNullOperator.DevNullOperatorFactory(0, planNodeIdAllocator.getNextId()),
                        new DevNullOperator.DevNullOperatorFactory(1, planNodeIdAllocator.getNextId())),
                OptionalInt.empty());
        CacheDriverFactory cacheDriverFactory = new CacheDriverFactory(
                TEST_SESSION,
                new TestPageSourceProvider(),
                registry,
                TEST_TABLE_HANDLE,
                signature,
                ImmutableMap.of(),
                () -> createStaticDynamicFilter(EMPTY),
                ImmutableList.of(driverFactory, driverFactory));
        DriverContext driverContext = createTaskContext(Executors.newSingleThreadExecutor(), Executors.newScheduledThreadPool(1), TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        // expect driver for original plan because cacheSplit is empty
        Driver driver = cacheDriverFactory.createDriver(driverContext, new ScheduledSplit(0, planNodeIdAllocator.getNextId(), split), Optional.empty());
        assertTrue(driver.getDriverContext().getCacheDriverContext().isEmpty());

        driverFactory = new DriverFactory(
                operatorIdAllocator.incrementAndGet(),
                true,
                false,
                ImmutableList.of(
                        new DevNullOperator.DevNullOperatorFactory(2, planNodeIdAllocator.getNextId()),
                        new DevNullOperator.DevNullOperatorFactory(3, planNodeIdAllocator.getNextId())),
                OptionalInt.empty());

        cacheDriverFactory = new CacheDriverFactory(
                TEST_SESSION,
                new TestPageSourceProvider(input -> TupleDomain.none()),
                registry,
                TEST_TABLE_HANDLE,
                signature,
                ImmutableMap.of(),
                () -> createStaticDynamicFilter(EMPTY),
                ImmutableList.of(driverFactory, driverFactory));

        // expect driver for original plan because dynamic filter filters data completely
        driver = cacheDriverFactory.createDriver(driverContext, new ScheduledSplit(0, planNodeIdAllocator.getNextId(), split), Optional.of(new CacheSplitId("split")));
        assertTrue(driver.getDriverContext().getCacheDriverContext().isEmpty());
    }

    @Test
    public void testCreateDriverWhenDynamicFilterWasChanged()
    {
        PlanSignature signature = new PlanSignature(new SignatureKey("sig"), Optional.empty(), ImmutableList.of(), TupleDomain.all(), TupleDomain.all());
        AtomicInteger operatorIdAllocator = new AtomicInteger();
        ConnectorSplit connectorSplit = new TestingSplit(false, ImmutableList.of());
        Split split = new Split(TEST_CATALOG_HANDLE, connectorSplit);
        ColumnHandle columnHandle = new TestingColumnHandle("column");
        TupleDomain<ColumnHandle> newDynamicPredicate = TupleDomain.withColumnDomains(ImmutableMap.of(columnHandle, Domain.create(ValueSet.of(BIGINT, 0L), false)));

        DriverFactory driverFactory = new DriverFactory(
                operatorIdAllocator.incrementAndGet(),
                true,
                false,
                ImmutableList.of(
                        new DevNullOperator.DevNullOperatorFactory(0, planNodeIdAllocator.getNextId()),
                        new DevNullOperator.DevNullOperatorFactory(1, planNodeIdAllocator.getNextId())),
                OptionalInt.empty());

        Map<ColumnHandle, CacheColumnId> dynamicFilterColumnMapping = ImmutableMap.of(columnHandle, new CacheColumnId("cacheColumnId"));
        CacheDriverFactory cacheDriverFactory = new CacheDriverFactory(
                TEST_SESSION,
                new TestPageSourceProvider(),
                registry,
                TEST_TABLE_HANDLE,
                signature,
                ImmutableMap.of(new CacheColumnId("cacheColumnId"), columnHandle),
                () -> createStaticDynamicFilter(new TestDynamicFilter(() -> newDynamicPredicate)),
                ImmutableList.of(driverFactory, driverFactory));
        DriverContext driverContext = createTaskContext(Executors.newSingleThreadExecutor(), Executors.newScheduledThreadPool(1), TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        // baseSignature's dynamic filters should be changed after creating a new driver while as new dynamic filter was supplied
        cacheDriverFactory.createDriver(driverContext, new ScheduledSplit(0, planNodeIdAllocator.getNextId(), split), Optional.of(new CacheSplitId("id")));
        assertThat(cacheDriverFactory.getCachePlanSignature().getDynamicPredicate()).isEqualTo(newDynamicPredicate.transformKeys(dynamicFilterColumnMapping::get));
    }

    private static class TestPageSourceProvider
            implements PageSourceProvider
    {
        private final Function<TupleDomain<ColumnHandle>, TupleDomain<ColumnHandle>> simplifyPredicateSupplier;

        public TestPageSourceProvider(Function<TupleDomain<ColumnHandle>, TupleDomain<ColumnHandle>> simplifyPredicateSupplier)
        {
            this.simplifyPredicateSupplier = simplifyPredicateSupplier;
        }

        public TestPageSourceProvider()
        {
            this(inputPredicate -> inputPredicate);
        }

        @Override
        public ConnectorPageSource createPageSource(
                Session session,
                Split split,
                TableHandle table,
                List<ColumnHandle> columns,
                DynamicFilter dynamicFilter)
        {
            return new FixedPageSource(rowPagesBuilder(BIGINT).build());
        }

        @Override
        public TupleDomain<ColumnHandle> simplifyPredicate(
                Session session,
                Split split,
                TableHandle table,
                TupleDomain<ColumnHandle> predicate)
        {
            return simplifyPredicateSupplier.apply(predicate);
        }
    }

    private static class TestDynamicFilter
            implements DynamicFilter
    {
        private final Supplier<TupleDomain<ColumnHandle>> dynamicPredicateSupplier;

        public TestDynamicFilter(Supplier<TupleDomain<ColumnHandle>> dynamicPredicateSupplier)
        {
            this.dynamicPredicateSupplier = dynamicPredicateSupplier;
        }

        @Override
        public Set<ColumnHandle> getColumnsCovered()
        {
            return ImmutableSet.of();
        }

        @Override
        public CompletableFuture<?> isBlocked()
        {
            return NOT_BLOCKED;
        }

        @Override
        public boolean isComplete()
        {
            return false;
        }

        @Override
        public boolean isAwaitable()
        {
            return false;
        }

        @Override
        public TupleDomain<ColumnHandle> getCurrentPredicate()
        {
            return dynamicPredicateSupplier.get();
        }

        @Override
        public long getPreferredDynamicFilterTimeout()
        {
            return 0;
        }
    }
}
