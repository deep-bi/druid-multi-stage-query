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

package org.apache.druid.msq.nql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.msq.exec.MSQTasks;
import org.apache.druid.msq.indexing.LegacyMSQSpec;
import org.apache.druid.msq.indexing.MSQNativeControllerTask;
import org.apache.druid.msq.indexing.MSQTuningConfig;
import org.apache.druid.msq.indexing.destination.MSQDestination;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.msq.util.TaskQueryMakerUtil;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContext;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.QueryResponse;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.sql.calcite.planner.ColumnMappings;

import java.util.Collections;
import java.util.Map;

public class MSQNativeTaskQueryMaker
{

  private final OverlordClient overlordClient;
  private final ObjectMapper jsonMapper;
  private final ColumnMappings columnMappings;
  private final RowSignature signature;


  public MSQNativeTaskQueryMaker(
      final OverlordClient overlordClient,
      final ObjectMapper jsonMapper,
      final ColumnMappings columnMappings,
      final RowSignature signature
  )
  {
    this.overlordClient = Preconditions.checkNotNull(overlordClient, "indexingServiceClient");
    this.jsonMapper = Preconditions.checkNotNull(jsonMapper, "jsonMapper");
    this.columnMappings = columnMappings;
    this.signature = signature;
  }


  public QueryResponse<Object[]> runNativeQuery(
      final Query<?> baseQuery,
      final AuthenticationResult authenticationResult
  )
  {
    String taskId = MSQTasks.controllerTaskId(baseQuery.getId());
    final QueryContext queryContext = baseQuery.context();

    final int maxNumWorkers = TaskQueryMakerUtil.getMaxNumWorkers(queryContext);
    final int rowsPerSegment = MultiStageQueryContext.getRowsPerSegment(queryContext);
    final int maxRowsInMemory = MultiStageQueryContext.getRowsInMemory(queryContext);
    final IndexSpec indexSpec = MultiStageQueryContext.getIndexSpec(queryContext, jsonMapper);
    final MSQDestination destination = TaskQueryMakerUtil.selectDestination(queryContext); // no export or table destination supported

    final Map<String, Object> nativeQueryContextOverrides = buildOverrideContext(queryContext, authenticationResult);

    final LegacyMSQSpec querySpec =
        LegacyMSQSpec.builder()
                     .query(baseQuery)
                     .queryContext(queryContext.override(nativeQueryContextOverrides))
                     .columnMappings(columnMappings)
                     .destination(destination)
                     .assignmentStrategy(MultiStageQueryContext.getAssignmentStrategy(queryContext))
                     .tuningConfig(new MSQTuningConfig(maxNumWorkers, maxRowsInMemory, rowsPerSegment, null, indexSpec))
                     .build();

    final MSQNativeControllerTask controllerTask = new MSQNativeControllerTask(
        taskId,
        querySpec,
        null,
        signature
    );
    FutureUtils.getUnchecked(overlordClient.runTask(taskId, controllerTask), true);
    return QueryResponse.withEmptyContext(Sequences.simple(Collections.singletonList(new Object[]{taskId})));
  }

  private static Map<String, Object> buildOverrideContext(
      final QueryContext queryContext,
      final AuthenticationResult authenticationResult
  )
  {
    return TaskQueryMakerUtil.buildOverrideContext(
        queryContext,
        authenticationResult.getIdentity(),
        false
    );
  }
}
