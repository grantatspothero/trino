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

import io.trino.security.AccessControl;
import io.trino.security.SecurityContext;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class ResultsCacheAnalyzerFactory
{
    private final AccessControl accessControl;

    @Inject
    public ResultsCacheAnalyzerFactory(AccessControl accessControl)
    {
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    public ResultsCacheAnalyzer createResultsCacheAnalyzer(SecurityContext securityContext)
    {
        return new ResultsCacheAnalyzer(accessControl, securityContext);
    }
}
