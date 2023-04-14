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

package io.trino.server.resultscache;

import io.airlift.log.Logger;
import io.trino.execution.QueryPreparer.PreparedQuery;
import io.trino.execution.ResultsCacheFinalResultConsumer;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.security.AccessControl;
import io.trino.security.SecurityContext;
import io.trino.spi.QueryId;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.tree.Query;

import static io.trino.server.resultscache.ResultsCacheEntry.ResultCacheFinalResult;
import static io.trino.server.resultscache.ResultsCacheEntry.ResultCacheFinalResult.ResultStatus.EXECUTE_STATEMENT;
import static io.trino.server.resultscache.ResultsCacheEntry.ResultCacheFinalResult.ResultStatus.NOT_SELECT;
import static io.trino.server.resultscache.ResultsCacheEntry.ResultCacheFinalResult.ResultStatus.QUERY_HAS_ABAC_RBAC_TABLE;
import static io.trino.server.resultscache.ResultsCacheEntry.ResultCacheFinalResult.ResultStatus.QUERY_HAS_SYSTEM_TABLE;
import static java.util.Objects.requireNonNull;

public class ResultsCacheAnalyzer
{
    private static final Logger log = Logger.get(ResultsCacheAnalyzer.class);
    private final AccessControl accessControl;
    private final SecurityContext securityContext;

    public ResultsCacheAnalyzer(AccessControl accessControl, SecurityContext securityContext)
    {
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.securityContext = requireNonNull(securityContext, "securityContext is null");
    }

    public boolean isStatementCacheable(ResultsCacheFinalResultConsumer resultsCacheFinalResultConsumer, QueryId queryId, PreparedQuery preparedQuery, Analysis analysis)
    {
        if (preparedQuery.isExecuteStatement()) {
            log.debug("QueryId: %s, statement is EXECUTE statement, not caching", queryId);
            resultsCacheFinalResultConsumer.setResultsCacheFinalResult(new ResultCacheFinalResult(EXECUTE_STATEMENT));
            return false;
        }

        if (!(preparedQuery.getStatement() instanceof Query)) {
            log.debug("QueryId: %s, statement is not a Query, not caching", queryId);
            resultsCacheFinalResultConsumer.setResultsCacheFinalResult(new ResultCacheFinalResult(NOT_SELECT));
            return false;
        }

        for (TableHandle tableHandle : analysis.getTables()) {
            switch (tableHandle.getCatalogHandle().getType()) {
                case INFORMATION_SCHEMA:
                case SYSTEM:
                    log.debug("QueryId: %s, query uses INFORMATION_SCHEMA or SYSTEM table %s, not caching", queryId, tableHandle);
                    resultsCacheFinalResultConsumer.setResultsCacheFinalResult(new ResultCacheFinalResult(QUERY_HAS_SYSTEM_TABLE));
                    return false;
                case NORMAL:
                    continue;
            }
        }

        for (QualifiedObjectName qualifiedObjectName : analysis.getTableNames()) {
            if (!accessControl.getRowFilters(securityContext, qualifiedObjectName).isEmpty()) {
                log.debug("QueryId: %s, query uses table: %s, which has row filters; not caching", queryId, qualifiedObjectName);
                resultsCacheFinalResultConsumer.setResultsCacheFinalResult(new ResultCacheFinalResult(QUERY_HAS_ABAC_RBAC_TABLE));
            }
        }

        log.debug("QueryId: %s, statement is cacheable", queryId);
        return true;
    }
}
