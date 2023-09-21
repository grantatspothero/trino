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
package io.trino.plugin.objectstore.scheduler;

import com.google.inject.Inject;
import io.starburst.stargate.accountwork.client.TrinoMaterializedViewScheduler;
import io.starburst.stargate.id.AccountWorkScheduleId;
import io.starburst.stargate.identity.DispatchSession;
import io.trino.plugin.iceberg.WorkScheduler;
import io.trino.spi.connector.ConnectorSession;

import java.util.Optional;

import static io.trino.plugin.base.galaxy.GalaxyIdentity.toDispatchSession;
import static java.util.Objects.requireNonNull;

public class GalaxyWorkScheduler
        implements WorkScheduler
{
    private final TrinoMaterializedViewScheduler materializedViewScheduler;

    @Inject
    public GalaxyWorkScheduler(TrinoMaterializedViewScheduler materializedViewScheduler)
    {
        this.materializedViewScheduler = requireNonNull(materializedViewScheduler, "materializedViewScheduler is null");
    }

    @Override
    public String createMaterializedViewRefreshJob(
            ConnectorSession session,
            String catalogName,
            String schemaName,
            String materializedViewName,
            String jobCron)
    {
        DispatchSession dispatchSession = toDispatchSession(session.getIdentity());
        return materializedViewScheduler.scheduleMaterializedViewRefresh(
                dispatchSession,
                catalogName,
                schemaName,
                materializedViewName,
                jobCron,
                dispatchSession.getRoleId()).toString();
    }

    @Override
    public Optional<String> getJobSchedule(ConnectorSession session, String jobId)
    {
        DispatchSession dispatchSession = toDispatchSession(session.getIdentity());
        return materializedViewScheduler.getScheduleForMaterializedViewRefresh(dispatchSession, new AccountWorkScheduleId(jobId));
    }

    @Override
    public void deleteJobSchedule(ConnectorSession session, String jobId)
    {
        DispatchSession dispatchSession = toDispatchSession(session.getIdentity());
        materializedViewScheduler.deleteMaterializedViewRefresh(dispatchSession, new AccountWorkScheduleId(jobId));
    }

    @Override
    public boolean updateJobSchedule(ConnectorSession session, String jobId, String jobCron)
    {
        DispatchSession dispatchSession = toDispatchSession(session.getIdentity());
        return materializedViewScheduler.updateMaterializedViewRefreshSchedule(
                dispatchSession,
                new AccountWorkScheduleId(jobId),
                jobCron);
    }

    @Override
    public boolean updateMaterializedViewName(ConnectorSession session, String jobId, String materializedViewName)
    {
        DispatchSession dispatchSession = toDispatchSession(session.getIdentity());
        return materializedViewScheduler.updateMaterializedViewName(
                dispatchSession,
                new AccountWorkScheduleId(jobId),
                materializedViewName);
    }
}
