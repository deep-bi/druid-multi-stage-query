/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.msq.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.msq.counters.CounterSnapshotsTree;
import org.apache.druid.msq.indexing.error.MSQErrorReport;
import org.apache.druid.msq.indexing.error.MSQFault;
import org.apache.druid.msq.indexing.report.MSQStagesReport;
import org.apache.druid.msq.indexing.report.MSQTaskReport;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.msq.sql.StatementState;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public abstract class AbstractResourceHelper implements ResultSequenceProvider
{
  @Nullable
  protected static MSQErrorReport getQueryExceptionDetails(@Nullable MSQTaskReportPayload payload)
  {
    return payload == null ? null : payload.getStatus().getErrorReport();
  }

  @Nullable
  public static List<MSQErrorReport> getQueryWarningDetails(@Nullable MSQTaskReportPayload payload)
  {
    return payload == null ? null : new ArrayList<>(payload.getStatus().getWarningReports());
  }

  @Nullable
  public static MSQStagesReport getQueryStagesReport(@Nullable MSQTaskReportPayload payload)
  {
    return payload == null ? null : payload.getStages();
  }

  @Nullable
  public static CounterSnapshotsTree getQueryCounters(@Nullable MSQTaskReportPayload payload)
  {
    return payload == null ? null : payload.getCounters();
  }

  @Nullable
  public static MSQTaskReportPayload getPayload(TaskReport.ReportMap reportMap)
  {
    if (reportMap == null) {
      return null;
    }

    Optional<MSQTaskReport> report = reportMap.findReport("multiStageQuery");
    return report.map(MSQTaskReport::getPayload).orElse(null);
  }

  protected static Map<String, String> buildExceptionContext(MSQFault fault, ObjectMapper mapper)
  {
    try {
      final Map<String, Object> msqFaultAsMap = new HashMap<>(
          mapper.readValue(
              mapper.writeValueAsBytes(fault),
              JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
          )
      );
      msqFaultAsMap.remove("errorCode");
      msqFaultAsMap.remove("errorMessage");

      final Map<String, String> exceptionContext = new HashMap<>();
      msqFaultAsMap.forEach((key, value) -> exceptionContext.put(key, String.valueOf(value)));

      return exceptionContext;
    }
    catch (Exception e) {
      throw DruidException.defensive("Could not read MSQFault[%s] as a map: [%s]", fault, e.getMessage());
    }
  }

  public static StatementState getSqlStatementState(TaskStatusPlus taskStatusPlus)
  {
    TaskState state = taskStatusPlus.getStatusCode();
    if (state == null) {
      return StatementState.ACCEPTED;
    }

    switch (state) {
      case FAILED:
        return StatementState.FAILED;
      case RUNNING:
        if (TaskLocation.unknown().equals(taskStatusPlus.getLocation())) {
          return StatementState.ACCEPTED;
        } else {
          return StatementState.RUNNING;
        }
      case SUCCESS:
        return StatementState.SUCCESS;
      default:
        throw new ISE("Unrecognized state[%s] found.", state);
    }
  }
}
