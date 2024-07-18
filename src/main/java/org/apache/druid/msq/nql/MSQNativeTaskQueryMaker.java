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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.msq.exec.MSQTasks;
import org.apache.druid.msq.indexing.MSQNativeControllerTask;
import org.apache.druid.msq.indexing.MSQSpec;
import org.apache.druid.msq.indexing.MSQTuningConfig;
import org.apache.druid.msq.indexing.destination.DataSourceMSQDestination;
import org.apache.druid.msq.indexing.destination.ExportMSQDestination;
import org.apache.druid.msq.indexing.destination.MSQDestination;
import org.apache.druid.msq.sql.MSQMode;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.msq.util.TaskQueryMakerUtil;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.QueryResponse;
import org.apache.druid.sql.calcite.parser.DruidSqlIngest;
import org.apache.druid.sql.calcite.parser.DruidSqlInsert;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.apache.druid.sql.destination.ExportDestination;
import org.apache.druid.sql.destination.IngestDestination;
import org.apache.druid.sql.destination.TableDestination;
import org.apache.druid.sql.http.ResultFormat;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MSQNativeTaskQueryMaker
{
  private static final Granularity DEFAULT_SEGMENT_GRANULARITY = Granularities.ALL;

  private final IngestDestination targetDataSource;
  private final OverlordClient overlordClient;
  private final ObjectMapper jsonMapper;
  private final ColumnMappings columnMappings;
  private final RowSignature signature;


  public MSQNativeTaskQueryMaker(
      @Nullable final IngestDestination targetDataSource,
      final OverlordClient overlordClient,
      final ObjectMapper jsonMapper,
      final ColumnMappings columnMappings,
      final RowSignature signature
  )
  {
    this.targetDataSource = targetDataSource;
    this.overlordClient = Preconditions.checkNotNull(overlordClient, "indexingServiceClient");
    this.jsonMapper = Preconditions.checkNotNull(jsonMapper, "jsonMapper");
    this.columnMappings = columnMappings;
    this.signature = signature;
  }


  public QueryResponse<Object[]> runNativeQuery(final Query<?> baseQuery)
  {
    String taskId = MSQTasks.controllerTaskId(baseQuery.getId());
    final QueryContext queryContext = baseQuery.context();
    final Map<String, Object> nativeQueryContext = new HashMap<>(queryContext.asMap());

    final String msqMode = MultiStageQueryContext.getMSQMode(queryContext);
    if (msqMode != null) {
      MSQMode.populateDefaultQueryContext(msqMode, nativeQueryContext);
    }

    Object segmentGranularity;
    try {
      segmentGranularity = Optional.ofNullable(queryContext.get(DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY))
                                   .orElse(jsonMapper.writeValueAsString(DEFAULT_SEGMENT_GRANULARITY));
    }
    catch (JsonProcessingException e) {
      // This would only be thrown if we are unable to serialize the DEFAULT_SEGMENT_GRANULARITY, which we don't expect
      // to happen
      throw DruidException.defensive()
                          .build(
                              e,
                              "Unable to deserialize the DEFAULT_SEGMENT_GRANULARITY in MSQTaskQueryMaker. "
                              + "This shouldn't have happened since the DEFAULT_SEGMENT_GRANULARITY object is guaranteed to be "
                              + "serializable. Please raise an issue in case you are seeing this message while executing a query."
                          );
    }

    final int maxNumTasks = MultiStageQueryContext.getMaxNumTasks(queryContext);

    if (maxNumTasks < 2) {
      throw InvalidInput.exception(
          "MSQ context maxNumTasks [%,d] cannot be less than 2, since at least 1 controller and 1 worker is necessary",
          maxNumTasks
      );
    }
    final int maxNumWorkers = maxNumTasks - 1;
    final int rowsPerSegment = MultiStageQueryContext.getRowsPerSegment(queryContext);
    final int maxRowsInMemory = MultiStageQueryContext.getRowsInMemory(queryContext);
    final IndexSpec indexSpec = MultiStageQueryContext.getIndexSpec(queryContext, jsonMapper);
    final boolean finalizeAggregations = MultiStageQueryContext.isFinalizeAggregations(queryContext);

    final List<Interval> replaceTimeChunks = TaskQueryMakerUtil.replaceTimeChunks(queryContext);

    final MSQDestination destination;

    if (targetDataSource instanceof ExportDestination) {
      ExportDestination exportDestination = ((ExportDestination) targetDataSource);
      ResultFormat format = ResultFormat.fromString(queryContext.getString(DruidSqlIngest.SQL_EXPORT_FILE_FORMAT));

      destination = new ExportMSQDestination(
          exportDestination.getStorageConnectorProvider(),
          format
      );
    } else if (targetDataSource instanceof TableDestination) {
      Granularity segmentGranularityObject;
      try {
        segmentGranularityObject = jsonMapper.readValue((String) segmentGranularity, Granularity.class);
      }
      catch (Exception e) {
        throw DruidException.defensive()
                            .build(
                                e,
                                "Unable to deserialize the provided segmentGranularity [%s]. "
                                + "This is populated internally by Druid and therefore should not occur. "
                                + "Please contact the developers if you are seeing this error message.",
                                segmentGranularity
                            );
      }

      final List<String> segmentSortOrder = MultiStageQueryContext.getSortOrder(queryContext);

      final DataSourceMSQDestination dataSourceMSQDestination = new DataSourceMSQDestination(
          targetDataSource.getDestinationName(),
          segmentGranularityObject,
          segmentSortOrder,
          replaceTimeChunks
      );
      MultiStageQueryContext.validateAndGetTaskLockType(
          queryContext,
          dataSourceMSQDestination.isReplaceTimeChunks()
      );
      destination = dataSourceMSQDestination;
    } else {
      destination = TaskQueryMakerUtil.selectDestination(queryContext);
    }
    final Map<String, Object> nativeQueryContextOverrides = new HashMap<>();

    // Add appropriate finalization to native query context.
    nativeQueryContextOverrides.put(QueryContexts.FINALIZE_KEY, finalizeAggregations);

    final MSQSpec querySpec =
        MSQSpec.builder()
               .query(baseQuery.withOverriddenContext(nativeQueryContextOverrides))
               .columnMappings(columnMappings)
               .destination(destination)
               .assignmentStrategy(MultiStageQueryContext.getAssignmentStrategy(queryContext))
               .tuningConfig(new MSQTuningConfig(maxNumWorkers, maxRowsInMemory, rowsPerSegment, indexSpec))
               .build();

    final MSQNativeControllerTask controllerTask = new MSQNativeControllerTask(
        taskId,
        querySpec.withOverriddenContext(nativeQueryContext),
        null,
        signature
    );
    FutureUtils.getUnchecked(overlordClient.runTask(taskId, controllerTask), true);
    return QueryResponse.withEmptyContext(Sequences.simple(Collections.singletonList(new Object[]{taskId})));
  }
}
