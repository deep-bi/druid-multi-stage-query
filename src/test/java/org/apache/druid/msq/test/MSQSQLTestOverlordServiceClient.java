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

package org.apache.druid.msq.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Injector;
import org.apache.druid.client.ImmutableSegmentLoadInfo;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.exec.ControllerImpl;
import org.apache.druid.msq.exec.ResultsContext;
import org.apache.druid.msq.exec.WorkerMemoryParameters;
import org.apache.druid.msq.indexing.MSQControllerTask;

import java.util.List;

public class MSQSQLTestOverlordServiceClient extends MSQTestOverlordServiceClient<MSQControllerTask>
{
  public MSQSQLTestOverlordServiceClient(
      ObjectMapper objectMapper,
      Injector injector,
      TaskActionClient taskActionClient,
      WorkerMemoryParameters workerMemoryParameters,
      List<ImmutableSegmentLoadInfo> loadedSegmentMetadata
  )
  {
    super(objectMapper, injector, taskActionClient, workerMemoryParameters, loadedSegmentMetadata);
  }

  @Override
  public ListenableFuture<Void> runTask(String taskId, Object taskObject)
  {
    TestQueryListener queryListener = null;
    ControllerImpl controller = null;
    MSQTestControllerContext msqTestControllerContext;
    try {
      MSQControllerTask cTask = objectMapper.convertValue(taskObject, MSQControllerTask.class);

      msqTestControllerContext = new MSQTestControllerContext(
          objectMapper,
          injector,
          taskActionClient,
          workerMemoryParameters,
          loadedSegmentMetadata,
          cTask.getTaskLockType(),
          cTask.getQuerySpec().getContext()
      );

      inMemoryControllerTask.put(cTask.getId(), cTask);

      controller = new ControllerImpl(
          cTask.getId(),
          cTask.getQuerySpec(),
          new ResultsContext(cTask.getSqlTypeNames(), cTask.getSqlResultsContext()),
          msqTestControllerContext
      );

      inMemoryControllers.put(controller.queryId(), controller);

      queryListener =
          new TestQueryListener(
              cTask.getId(),
              cTask.getQuerySpec().getDestination()
          );

      try {
        controller.run(queryListener);
        inMemoryTaskStatus.put(taskId, queryListener.getStatusReport().toTaskStatus(cTask.getId()));
      }
      catch (Exception e) {
        inMemoryTaskStatus.put(taskId, TaskStatus.failure(cTask.getId(), e.toString()));
      }
      return Futures.immediateFuture(null);
    }
    catch (Exception e) {
      throw new ISE(e, "Unable to run");
    }
    finally {
      if (queryListener != null && queryListener.reportMap != null) {
        reports.put(controller.queryId(), queryListener.getReportMap());
      }
    }
  }

  @Override
  protected String getTaskType()
  {
    return MSQControllerTask.TYPE;
  }
}
