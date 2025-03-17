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
import org.apache.druid.msq.exec.NativeControllerImpl;
import org.apache.druid.msq.exec.WorkerMemoryParameters;
import org.apache.druid.msq.indexing.MSQNativeControllerTask;

import java.util.List;

public class MSQNativeTestOverlordServiceClient extends MSQTestOverlordServiceClient<MSQNativeControllerTask>
{
  public MSQNativeTestOverlordServiceClient(
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
    NativeControllerImpl controller = null;
    MSQTestControllerContext msqTestControllerContext;
    try {
      MSQNativeControllerTask cTask = objectMapper.convertValue(taskObject, MSQNativeControllerTask.class);

      msqTestControllerContext = new MSQTestControllerContext(
          objectMapper,
          injector,
          taskActionClient,
          workerMemoryParameters,
          loadedSegmentMetadata,
          cTask.getTaskLockType(),
          cTask.getQuerySpec().getQuery().context()
      );

      inMemoryControllerTask.put(cTask.getId(), cTask);

      controller = new NativeControllerImpl(
          cTask.getId(),
          cTask.getQuerySpec(),
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
      if (controller != null && queryListener != null) {
        reports.put(controller.queryId(), queryListener.getReportMap());
      }
    }
  }

  @Override
  protected String getTaskType()
  {
    return MSQNativeControllerTask.TYPE;
  }
}
