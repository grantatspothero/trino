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

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.ProvidesIntoSet;
import io.trino.connector.system.FlushHistoryBasedStatsCacheProcedure;
import io.trino.spi.procedure.Procedure;
import io.trino.sql.PlannerContext;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.List;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class StatsCalculatorModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(StatsNormalizer.class).in(Scopes.SINGLETON);
        binder.bind(ScalarStatsCalculator.class).in(Scopes.SINGLETON);
        binder.bind(FilterStatsCalculator.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, new TypeLiteral<List<ComposableStatsCalculator.Rule<?>>>() {})
                .setDefault().toProvider(StatsRulesProvider.class).in(Scopes.SINGLETON);
        binder.bind(ComposableStatsCalculator.class).in(Scopes.SINGLETON);
        binder.bind(HistoryBasedStatsCalculator.class).in(Scopes.SINGLETON);
        binder.bind(StatsCalculator.class).to(HistoryBasedStatsCalculator.class);
        binder.bind(StatsCalculator.class).annotatedWith(NoHistoryBasedStats.class).to(ComposableStatsCalculator.class);
        binder.bind(FlushHistoryBasedStatsCacheProcedure.class).in(Scopes.SINGLETON);

        newExporter(binder).export(HistoryBasedStatsCalculator.class)
                .as(generator -> generator.generatedNameOf(HistoryBasedStatsCalculator.class));
    }

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    public @interface NoHistoryBasedStats
    {
    }

    public static class StatsRulesProvider
            implements Provider<List<ComposableStatsCalculator.Rule<?>>>
    {
        private final PlannerContext plannerContext;
        private final ScalarStatsCalculator scalarStatsCalculator;
        private final FilterStatsCalculator filterStatsCalculator;
        private final StatsNormalizer normalizer;

        @Inject
        public StatsRulesProvider(PlannerContext plannerContext, ScalarStatsCalculator scalarStatsCalculator, FilterStatsCalculator filterStatsCalculator, StatsNormalizer normalizer)
        {
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
            this.scalarStatsCalculator = requireNonNull(scalarStatsCalculator, "scalarStatsCalculator is null");
            this.filterStatsCalculator = requireNonNull(filterStatsCalculator, "filterStatsCalculator is null");
            this.normalizer = requireNonNull(normalizer, "normalizer is null");
        }

        @Override
        public List<ComposableStatsCalculator.Rule<?>> get()
        {
            ImmutableList.Builder<ComposableStatsCalculator.Rule<?>> rules = ImmutableList.builder();

            rules.add(new OutputStatsRule());
            rules.add(new TableScanStatsRule(normalizer));
            rules.add(new SimpleFilterProjectSemiJoinStatsRule(plannerContext.getMetadata(), normalizer, filterStatsCalculator)); // this must be before FilterStatsRule
            rules.add(new FilterProjectAggregationStatsRule(normalizer, filterStatsCalculator)); // this must be before FilterStatsRule
            rules.add(new FilterStatsRule(normalizer, filterStatsCalculator));
            rules.add(new ValuesStatsRule(plannerContext));
            rules.add(new LimitStatsRule(normalizer));
            rules.add(new DistinctLimitStatsRule(normalizer));
            rules.add(new TopNStatsRule(normalizer));
            rules.add(new EnforceSingleRowStatsRule(normalizer));
            rules.add(new ProjectStatsRule(scalarStatsCalculator, normalizer));
            rules.add(new ExchangeStatsRule(normalizer));
            rules.add(new JoinStatsRule(filterStatsCalculator, normalizer));
            rules.add(new SpatialJoinStatsRule(filterStatsCalculator, normalizer));
            rules.add(new AggregationStatsRule(normalizer));
            rules.add(new UnionStatsRule(normalizer));
            rules.add(new AssignUniqueIdStatsRule());
            rules.add(new SemiJoinStatsRule());
            rules.add(new RowNumberStatsRule(normalizer));
            rules.add(new SampleStatsRule(normalizer));
            rules.add(new SortStatsRule());
            rules.add(new ChooseAlternativeRule(normalizer));

            return rules.build();
        }
    }

    @ProvidesIntoSet
    public static Procedure getFlushHistoryBasedStatsCacheProcedure(FlushHistoryBasedStatsCacheProcedure procedure)
    {
        return procedure.get();
    }
}
