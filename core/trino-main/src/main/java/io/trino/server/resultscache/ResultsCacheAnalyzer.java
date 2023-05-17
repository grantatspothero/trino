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

import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.security.AccessControl;
import io.trino.security.SecurityContext;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.Statement;

import static java.util.Objects.requireNonNull;

public class ResultsCacheAnalyzer
{
    private final AccessControl accessControl;
    private final SecurityContext securityContext;

    public ResultsCacheAnalyzer(AccessControl accessControl, SecurityContext securityContext)
    {
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.securityContext = requireNonNull(securityContext, "securityContext is null");
    }

    public boolean isStatementCacheable(Statement originalStatement, Analysis analysis)
    {
        if (originalStatement instanceof Query) {
            for (TableHandle tableHandle : analysis.getTables()) {
                switch (tableHandle.getCatalogHandle().getType()) {
                    case INFORMATION_SCHEMA:
                    case SYSTEM:
                        return false;
                    case NORMAL:
                        continue;
                }
            }

            for (QualifiedObjectName qualifiedObjectName : analysis.getTableNames()) {
                if (!accessControl.getRowFilters(securityContext, qualifiedObjectName).isEmpty()) {
                    return false;
                }
            }

            return true;
        }
        return false;
    }
}
