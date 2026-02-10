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

import com.google.common.base.Preconditions;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.frame.FrameType;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.msq.indexing.destination.DurableStorageMSQDestination;
import org.apache.druid.msq.indexing.destination.ExportMSQDestination;
import org.apache.druid.msq.indexing.destination.MSQDestination;
import org.apache.druid.msq.indexing.destination.MSQSelectDestination;
import org.apache.druid.msq.indexing.destination.TaskReportMSQDestination;
import org.apache.druid.msq.sql.MSQMode;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.sql.calcite.parser.DruidSqlIngest;
import org.apache.druid.sql.destination.ExportDestination;
import org.apache.druid.sql.http.ResultFormat;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class TaskQueryMakerUtil
{
  public static final Granularity DEFAULT_SEGMENT_GRANULARITY = Granularities.ALL;
  public static final String USER_KEY = "__user";

  public static MSQDestination selectDestination(final QueryContext queryContext)
  {
    final MSQSelectDestination msqSelectDestination = MultiStageQueryContext.getSelectDestination(queryContext);
    if (msqSelectDestination.equals(MSQSelectDestination.TASKREPORT)) {
      return TaskReportMSQDestination.instance();
    } else if (msqSelectDestination.equals(MSQSelectDestination.DURABLESTORAGE)) {
      return DurableStorageMSQDestination.instance();
    } else {
      throw InvalidInput.exception(
          "Unsupported select destination [%s] provided in the query context. MSQ can currently write the select results to "
          + "[%s]",
          msqSelectDestination.getName(),
          Arrays.stream(MSQSelectDestination.values())
                .map(MSQSelectDestination::getName)
                .collect(Collectors.joining(","))
      );
    }
  }


  public static MSQDestination buildExportDestination(ExportDestination targetDataSource, QueryContext sqlQueryContext)
  {
    ResultFormat format = ResultFormat.fromString(sqlQueryContext.getString(DruidSqlIngest.SQL_EXPORT_FILE_FORMAT));

    return new ExportMSQDestination(
        targetDataSource.getStorageConnectorProvider(),
        format
    );
  }

  public static Map<String, Object> buildOverrideContext(
      final QueryContext baseContext,
      final String userIdentity,
      final boolean isReindex
  )
  {
    Preconditions.checkNotNull(baseContext, "baseContext");

    final boolean finalizeAggregations = MultiStageQueryContext.isFinalizeAggregations(baseContext);

    final Map<String, Object> overrides = new HashMap<>();

    // Add appropriate finalization to native query context.
    overrides.put(QueryContexts.FINALIZE_KEY, finalizeAggregations);
    // This flag is to ensure backward compatibility, as brokers are upgraded after indexers/middlemanagers.
    overrides.put(MultiStageQueryContext.WINDOW_FUNCTION_OPERATOR_TRANSFORMATION, true);

    if (isReindex) {
      overrides.put(MultiStageQueryContext.CTX_IS_REINDEX, true);
    }

    overrides.putAll(baseContext.asMap());

    // adding user
    overrides.put(TaskQueryMakerUtil.USER_KEY, userIdentity);

    final String msqMode = MultiStageQueryContext.getMSQMode(baseContext);
    if (msqMode != null) {
      MSQMode.populateDefaultQueryContext(msqMode, overrides);
    }

    // Use the latest row-based frame type. The default is an older type, to ensure compatibility during rolling
    // updates. Since the Broker is updated last, it's safe to set this property on the Broker.
    overrides.putIfAbsent(
        MultiStageQueryContext.CTX_ROW_BASED_FRAME_TYPE,
        (int) FrameType.latestRowBased().version()
    );

    // Add the start time.
    overrides.put(MultiStageQueryContext.CTX_START_TIME, DateTimes.nowUtc().toString());

    return overrides;
  }

  public static int getMaxNumWorkers(final QueryContext queryContext)
  {
    Preconditions.checkNotNull(queryContext, "queryContext");
    final int maxNumTasks = MultiStageQueryContext.getMaxNumTasks(queryContext);

    if (maxNumTasks < 2) {
      throw org.apache.druid.error.InvalidInput.exception(
          "MSQ context maxNumTasks [%,d] cannot be less than 2, since at least 1 controller and 1 worker is necessary",
          maxNumTasks
      );
    }

    return maxNumTasks - 1;
  }
}
