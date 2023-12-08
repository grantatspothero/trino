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
package io.trino.plugin.jdbc;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.airlift.configuration.LegacyConfig;
import io.trino.spi.connector.ConnectorSession;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

@DefunctConfig("allow-drop-table")
public class JdbcMetadataConfig
{
    public enum ListColumnsMode
    {
        /**
         * Traditional / legacy / battle tested mode.
         */
        CLASSIC,

        /**
         * Like {@link #CLASSIC}, but processes tables in multiple threads.
         */
        PARALLEL,

        /**
         * Uses {@link JdbcClient#getAllTableColumns(ConnectorSession, Optional)}.
         */
        DMA,

        /**
         * Uses {@link JdbcClient#getAllTableColumns(ConnectorSession, Optional)}
         * and processes schemas in multiple threads.
         */
        DMA_P,
    }

    public enum ListCommentsMode
    {
        /**
         * Traditional / legacy / battle tested mode.
         */
        CLASSIC,

        /**
         * Uses {@link JdbcClient#getAllTableComments(ConnectorSession, Optional)}
         * <p>
         * <em>DMA</em> stands for Direct Metadata Access.
         */
        DMA,

        /**
         * Uses {@link JdbcClient#getAllTableComments(ConnectorSession, Optional)}
         * and processes schemas in multiple threads.
         */
        // TODO this probably isn't worth keeping around. Test on various connectors and remove the implementation if is not significantly better than DMA
        DMA_P,
    }

    private boolean complexExpressionPushdownEnabled = true;
    /*
     * Join pushdown is disabled by default as this is the safer option.
     * Pushing down a join which substantially increases the row count vs
     * sizes of left and right table separately, may incur huge cost both
     * in terms of performance and money due to an increased network traffic.
     */
    private boolean joinPushdownEnabled;
    private boolean complexJoinPushdownEnabled = true;
    private boolean aggregationPushdownEnabled = true;

    private boolean topNPushdownEnabled = true;

    private ListColumnsMode listColumnsMode = ListColumnsMode.CLASSIC; // default overridden in connectors that support other modes
    private ListCommentsMode listCommentsMode = ListCommentsMode.DMA;
    // This is for IO, so default value not based on number of cores.
    // This ~limits number of concurrent queries to the remote database
    private int maxMetadataBackgroundProcessingThreads = 8;

    // Pushed domains are transformed into SQL IN lists
    // (or sequence of range predicates) in JDBC connectors.
    // Too large IN lists cause significant performance regression.
    // Use 32 as compaction threshold as it provides reasonable balance
    // between performance and pushdown capabilities
    private int domainCompactionThreshold = 32;

    public boolean isComplexExpressionPushdownEnabled()
    {
        return complexExpressionPushdownEnabled;
    }

    @Config("complex-expression-pushdown.enabled")
    public JdbcMetadataConfig setComplexExpressionPushdownEnabled(boolean complexExpressionPushdownEnabled)
    {
        this.complexExpressionPushdownEnabled = complexExpressionPushdownEnabled;
        return this;
    }

    public boolean isJoinPushdownEnabled()
    {
        return joinPushdownEnabled;
    }

    @LegacyConfig("experimental.join-pushdown.enabled")
    @Config("join-pushdown.enabled")
    @ConfigDescription("Enable join pushdown")
    public JdbcMetadataConfig setJoinPushdownEnabled(boolean joinPushdownEnabled)
    {
        this.joinPushdownEnabled = joinPushdownEnabled;
        return this;
    }

    public boolean isComplexJoinPushdownEnabled()
    {
        return complexJoinPushdownEnabled;
    }

    @Config("join-pushdown.with-expressions")
    @ConfigDescription("Enable join pushdown with complex expressions")
    public JdbcMetadataConfig setComplexJoinPushdownEnabled(boolean complexJoinPushdownEnabled)
    {
        this.complexJoinPushdownEnabled = complexJoinPushdownEnabled;
        return this;
    }

    public boolean isAggregationPushdownEnabled()
    {
        return aggregationPushdownEnabled;
    }

    @Config("aggregation-pushdown.enabled")
    @LegacyConfig("allow-aggregation-pushdown")
    @ConfigDescription("Enable aggregation pushdown")
    public JdbcMetadataConfig setAggregationPushdownEnabled(boolean aggregationPushdownEnabled)
    {
        this.aggregationPushdownEnabled = aggregationPushdownEnabled;
        return this;
    }

    @Config("topn-pushdown.enabled")
    @ConfigDescription("Enable TopN pushdown")
    public JdbcMetadataConfig setTopNPushdownEnabled(boolean enabled)
    {
        this.topNPushdownEnabled = enabled;
        return this;
    }

    public Boolean isTopNPushdownEnabled()
    {
        return this.topNPushdownEnabled;
    }

    @NotNull
    public ListColumnsMode getListColumnsMode()
    {
        return listColumnsMode;
    }

    @Config("jdbc.list-columns-mode")
    @ConfigDescription("Select implementation for listing tables' columns")
    public JdbcMetadataConfig setListColumnsMode(ListColumnsMode listColumnsMode)
    {
        this.listColumnsMode = listColumnsMode;
        return this;
    }

    @NotNull
    public ListCommentsMode getListCommentsMode()
    {
        return listCommentsMode;
    }

    @Config("jdbc.list-comments-mode")
    @ConfigDescription("Select implementation for listing tables' comments")
    public JdbcMetadataConfig setListCommentsMode(ListCommentsMode listCommentsMode)
    {
        this.listCommentsMode = listCommentsMode;
        return this;
    }

    @Min(0)
    public int getMaxMetadataBackgroundProcessingThreads()
    {
        return maxMetadataBackgroundProcessingThreads;
    }

    @Config("jdbc.metadata-background-threads")
    public JdbcMetadataConfig setMaxMetadataBackgroundProcessingThreads(int maxMetadataBackgroundProcessingThreads)
    {
        this.maxMetadataBackgroundProcessingThreads = maxMetadataBackgroundProcessingThreads;
        return this;
    }

    @Min(1)
    public int getDomainCompactionThreshold()
    {
        return domainCompactionThreshold;
    }

    @Config("domain-compaction-threshold")
    @ConfigDescription("Maximum ranges to allow in a tuple domain without compacting it")
    public JdbcMetadataConfig setDomainCompactionThreshold(int domainCompactionThreshold)
    {
        this.domainCompactionThreshold = domainCompactionThreshold;
        return this;
    }
}
