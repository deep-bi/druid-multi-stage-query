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
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.exec.NativeControllerImpl;
import org.apache.druid.msq.exec.WorkerMemoryParameters;
import org.apache.druid.msq.indexing.MSQNativeControllerTask;
import org.apache.druid.msq.sql.MSQTaskQueryKitSpecFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MSQNativeTestOverlordServiceClient
    extends MSQTestOverlordServiceClient<MSQNativeControllerTask, NativeControllerImpl>
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
    MSQTestTaskDetails testTaskDetails = registerTestTask(taskId);
    try {
      MSQNativeControllerTask cTask = objectMapper.convertValue(taskObject, MSQNativeControllerTask.class);

      msqTestControllerContext = new MSQTestControllerContext(
          cTask.getId(),
          objectMapper,
          injector,
          taskActionClient,
          workerMemoryParameters,
          loadedSegmentMetadata,
          cTask.getTaskLockType(),
          cTask.getQuerySpec().getContext(),
          emitter
      );

      assertEquals(taskId, cTask.getId());
      testTaskDetails.controllerTask = cTask;

      controller = new NativeControllerImpl(
          cTask.getQuerySpec(),
          msqTestControllerContext,
          injector.getInstance(MSQTaskQueryKitSpecFactory.class)
      );

      testTaskDetails.addController(controller);

      queryListener =
          new TestQueryListener(
              cTask.getId(),
              cTask.getQuerySpec().getDestination()
          );

      try {
        controller.run(queryListener);
        testTaskDetails.taskStatus = queryListener.getStatusReport().toTaskStatus(cTask.getId());
      }
      catch (Exception e) {
        testTaskDetails.taskStatus = TaskStatus.failure(cTask.getId(), e.toString());
      }
      return Futures.immediateFuture(null);
    }
    catch (Exception e) {
      throw new ISE(e, "Unable to run");
    }
    finally {
      if (queryListener != null && queryListener.reportMap != null) {
        testTaskDetails.report = queryListener.getReportMap();
      }
    }
  }

  @Override
  protected String getTaskType()
  {
    return MSQNativeControllerTask.TYPE;
  }

  @Override
  protected Logger getLogger()
  {
    return new Logger(MSQNativeTestOverlordServiceClient.class);
  }
}
