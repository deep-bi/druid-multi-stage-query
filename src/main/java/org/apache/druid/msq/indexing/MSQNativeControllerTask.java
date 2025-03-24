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

package org.apache.druid.msq.indexing;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.apache.druid.client.indexing.ClientTaskQuery;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.AbstractTask;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.msq.exec.Controller;
import org.apache.druid.msq.exec.ControllerContext;
import org.apache.druid.msq.exec.MSQTasks;
import org.apache.druid.msq.exec.NativeControllerImpl;
import org.apache.druid.msq.indexing.destination.DataSourceMSQDestination;
import org.apache.druid.msq.indexing.destination.MSQDestination;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.StandardRetryPolicy;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.security.ResourceAction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;

@JsonTypeName(MSQNativeControllerTask.TYPE)
public class MSQNativeControllerTask extends AbstractTask implements ClientTaskQuery, IsMSQTask
{

  public static final String TYPE = "native_query_controller";
  public static final String DUMMY_DATASOURCE_FOR_SELECT = "__query_select";

  private final MSQSpec querySpec;
  private final RowSignature signature;
  @JacksonInject
  private Injector injector;
  private volatile Controller controller;

  public MSQNativeControllerTask(
      @JsonProperty("id") @Nullable String id,
      @JsonProperty("spec") MSQSpec querySpec,
      @JsonProperty("context") @Nullable Map<String, Object> context,
      @JsonProperty("signature") @Nullable RowSignature signature
  )
  {
    super(
        id != null ? id : MSQTasks.controllerTaskId(null),
        id,
        null,
        getDataSourceForTaskMetadata(querySpec),
        context
    );
    addToContext(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, true); // mb part to remove
    this.querySpec = querySpec;
    this.signature = signature;
  }

  public MSQNativeControllerTask(
      @Nullable String id,
      MSQSpec querySpec,
      @Nullable Map<String, Object> context,
      @Nullable RowSignature signature,
      Injector injector
  )
  {
    this(id, querySpec, context, signature);
    this.injector = injector;
  }

  private static String getDataSourceForTaskMetadata(final MSQSpec querySpec)
  {
    final MSQDestination destination = querySpec.getDestination();

    if (destination instanceof DataSourceMSQDestination) {
      return ((DataSourceMSQDestination) destination).getDataSource();
    } else {
      return DUMMY_DATASOURCE_FOR_SELECT;
    }
  }

  @Override
  public TaskStatus runTask(final TaskToolbox toolbox) throws Exception
  {
    final ServiceClientFactory clientFactory =
        injector.getInstance(Key.get(ServiceClientFactory.class, EscalatedGlobal.class));
    final OverlordClient overlordClient = injector.getInstance(OverlordClient.class)
                                                  .withRetryPolicy(StandardRetryPolicy.unlimited());
    final ControllerContext context = new IndexerControllerContext(
        this,
        toolbox,
        injector,
        clientFactory,
        overlordClient
    );
    controller = new NativeControllerImpl(this.getId(), querySpec, context);

    final TaskReportQueryListener queryListener = new TaskReportQueryListener(
        querySpec.getDestination(),
        () -> toolbox.getTaskReportFileWriter().openReportOutputStream(getId()),
        toolbox.getJsonMapper(),
        getId(),
        getContext(),
        true
    );

    controller.run(queryListener);
    return queryListener.getStatusReport().toTaskStatus(getId());
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Nonnull
  @JsonIgnore
  @Override
  public Set<ResourceAction> getInputSourceResources()
  {
    // the input sources are properly computed in the SQL / calcite layer, but not in the native MSQ task here.
    return ImmutableSet.of();
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient)
  {
    return true;
  }

  @Override
  @JsonProperty("spec")
  public MSQSpec getQuerySpec()
  {
    return querySpec;
  }

  @JsonProperty("signature")
  public RowSignature getSignature()
  {
    return signature;
  }

  @Override
  public void stopGracefully(TaskConfig taskConfig)
  {
    if (controller != null) {
      controller.stop();
    }
  }

}
