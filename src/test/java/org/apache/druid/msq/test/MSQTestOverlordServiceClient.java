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
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Injector;
import org.apache.druid.client.ImmutableSegmentLoadInfo;
import org.apache.druid.client.indexing.ClientTaskQuery;
import org.apache.druid.client.indexing.TaskPayloadResponse;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.task.AbstractTask;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.msq.exec.Controller;
import org.apache.druid.msq.exec.WorkerMemoryParameters;
import org.apache.druid.rpc.indexing.NoopOverlordClient;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class MSQTestOverlordServiceClient<TaskType extends AbstractTask & ClientTaskQuery>
    extends NoopOverlordClient
{
  public static final DateTime CREATED_TIME = DateTimes.of("2023-05-31T12:00Z");
  public static final DateTime QUEUE_INSERTION_TIME = DateTimes.of("2023-05-31T12:01Z");
  public static final long DURATION = 100L;
  protected final Injector injector;
  protected final ObjectMapper objectMapper;
  protected final TaskActionClient taskActionClient;
  protected final WorkerMemoryParameters workerMemoryParameters;
  protected final List<ImmutableSegmentLoadInfo> loadedSegmentMetadata;
  protected final Map<String, Controller> inMemoryControllers = new HashMap<>();
  protected final Map<String, TaskReport.ReportMap> reports = new HashMap<>();
  protected final Map<String, TaskType> inMemoryControllerTask = new HashMap<>();
  protected final Map<String, TaskStatus> inMemoryTaskStatus = new HashMap<>();

  public MSQTestOverlordServiceClient(
      ObjectMapper objectMapper,
      Injector injector,
      TaskActionClient taskActionClient,
      WorkerMemoryParameters workerMemoryParameters,
      List<ImmutableSegmentLoadInfo> loadedSegmentMetadata
  )
  {
    this.objectMapper = objectMapper;
    this.injector = injector;
    this.taskActionClient = taskActionClient;
    this.workerMemoryParameters = workerMemoryParameters;
    this.loadedSegmentMetadata = loadedSegmentMetadata;
  }

  @Override
  public abstract ListenableFuture<Void> runTask(String taskId, Object taskObject);

  @Override
  public ListenableFuture<Void> cancelTask(String taskId)
  {
    inMemoryControllers.get(taskId).stop();
    return Futures.immediateFuture(null);
  }

  @Override
  public ListenableFuture<TaskReport.ReportMap> taskReportAsMap(String taskId)
  {
    return Futures.immediateFuture(getReportForTask(taskId));
  }

  @Override
  public ListenableFuture<TaskPayloadResponse> taskPayload(String taskId)
  {
    SettableFuture<TaskPayloadResponse> future = SettableFuture.create();
    future.set(new TaskPayloadResponse(taskId, getMSQControllerTask(taskId)));
    return future;
  }

  @Override
  public ListenableFuture<TaskStatusResponse> taskStatus(String taskId)
  {
    SettableFuture<TaskStatusResponse> future = SettableFuture.create();
    TaskStatus taskStatus = inMemoryTaskStatus.get(taskId);
    future.set(new TaskStatusResponse(taskId, new TaskStatusPlus(
        taskId,
        null,
        getTaskType(),
        CREATED_TIME,
        QUEUE_INSERTION_TIME,
        taskStatus.getStatusCode(),
        null,
        DURATION,
        taskStatus.getLocation(),
        null,
        taskStatus.getErrorMsg()
    )));

    return future;
  }

  // hooks to pull stuff out for testing
  @Nullable
  public TaskReport.ReportMap getReportForTask(String id)
  {
    return reports.get(id);
  }

  @Nullable
  TaskType getMSQControllerTask(String id)
  {
    return inMemoryControllerTask.get(id);
  }

  protected abstract String getTaskType();
}
