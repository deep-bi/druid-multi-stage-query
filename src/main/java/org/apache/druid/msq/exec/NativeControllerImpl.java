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

package org.apache.druid.msq.exec;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.allocation.ArenaMemoryAllocator;
import org.apache.druid.frame.processor.FrameProcessorExecutor;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.msq.counters.CounterSnapshotsTree;
import org.apache.druid.msq.indexing.InputChannelFactory;
import org.apache.druid.msq.indexing.InputChannelsImpl;
import org.apache.druid.msq.indexing.MSQControllerTask;
import org.apache.druid.msq.indexing.MSQNativeControllerTask;
import org.apache.druid.msq.indexing.MSQSpec;
import org.apache.druid.msq.indexing.MSQTuningConfig;
import org.apache.druid.msq.indexing.MSQWorkerTaskLauncher;
import org.apache.druid.msq.indexing.destination.DurableStorageMSQDestination;
import org.apache.druid.msq.indexing.destination.ExportMSQDestination;
import org.apache.druid.msq.indexing.destination.MSQSelectDestination;
import org.apache.druid.msq.indexing.destination.TaskReportMSQDestination;
import org.apache.druid.msq.indexing.error.CannotParseExternalDataFault;
import org.apache.druid.msq.indexing.error.FaultsExceededChecker;
import org.apache.druid.msq.indexing.error.MSQErrorReport;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.indexing.error.MSQWarnings;
import org.apache.druid.msq.indexing.error.QueryNotSupportedFault;
import org.apache.druid.msq.indexing.error.UnknownFault;
import org.apache.druid.msq.indexing.report.MSQResultsReport;
import org.apache.druid.msq.input.InputSpecSlicerFactory;
import org.apache.druid.msq.input.stage.InputChannels;
import org.apache.druid.msq.input.stage.StageInputSpec;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.QueryDefinitionBuilder;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.kernel.controller.ControllerQueryKernel;
import org.apache.druid.msq.querykit.QueryKit;
import org.apache.druid.msq.querykit.ShuffleSpecFactory;
import org.apache.druid.msq.querykit.results.ExportResultsFrameProcessorFactory;
import org.apache.druid.msq.shuffle.input.DurableStorageInputChannelFactory;
import org.apache.druid.msq.shuffle.input.WorkerInputChannelFactory;
import org.apache.druid.msq.util.ControllerUtil;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.msq.util.NativeStatementResourceHelper;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContext;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.storage.ExportStorageProvider;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class NativeControllerImpl extends AbstractController<MSQNativeControllerTask>
{

  public NativeControllerImpl(
      MSQNativeControllerTask task,
      ControllerContext context
  )
  {
    super(task, context, MultiStageQueryContext.isDurableStorageEnabled(
        task.getQuerySpec().getQuery().context()
    ), MultiStageQueryContext.isFaultToleranceEnabled(
        task.getQuerySpec().getQuery().context()
    ), MultiStageQueryContext.isFailOnEmptyInsertEnabled(
        task.getQuerySpec().getQuery().context()
    ));
  }


  private static boolean isInlineResults(final MSQSpec querySpec)
  {
    return querySpec.getDestination() instanceof TaskReportMSQDestination
           || querySpec.getDestination() instanceof DurableStorageMSQDestination;
  }

  @SuppressWarnings("unchecked")
  private static QueryDefinition makeQueryDefinition(
      final String queryId,
      @SuppressWarnings("rawtypes") final QueryKit toolKit,
      final MSQSpec querySpec
  )
  {
    final MSQTuningConfig tuningConfig = querySpec.getTuningConfig();
    final Query<?> queryToPlan;
    final ShuffleSpecFactory shuffleSpecFactory;

    shuffleSpecFactory = querySpec.getDestination()
                                  .getShuffleSpecFactory(MultiStageQueryContext.getRowsPerPage(querySpec.getQuery()
                                                                                                        .context()));
    queryToPlan = querySpec.getQuery();


    final QueryDefinition queryDef;

    try {
      queryDef = toolKit.makeQueryDefinition(
          queryId,
          queryToPlan,
          toolKit,
          shuffleSpecFactory,
          tuningConfig.getMaxNumWorkers(),
          0
      );
    }
    catch (MSQException e) {
      // If the toolkit throws a MSQFault, don't wrap it in a more generic QueryNotSupportedFault
      throw e;
    }
    catch (Exception e) {
      throw new MSQException(e, QueryNotSupportedFault.INSTANCE);
    }

    if (querySpec.getDestination() instanceof ExportMSQDestination) {
      final ExportMSQDestination exportMSQDestination = (ExportMSQDestination) querySpec.getDestination();
      final ExportStorageProvider exportStorageProvider = exportMSQDestination.getExportStorageProvider();

      try {
        // Check that the export destination is empty as a sanity check. We want to avoid modifying any other files with export.
        Iterator<String> filesIterator = exportStorageProvider.get().listDir("");
        if (filesIterator.hasNext()) {
          throw DruidException.forPersona(DruidException.Persona.USER)
                              .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                              .build(
                                  "Found files at provided export destination[%s]. Export is only allowed to "
                                  + "an empty path. Please provide an empty path/subdirectory or move the existing files.",
                                  exportStorageProvider.getBasePath()
                              );
        }
      }
      catch (IOException e) {
        throw DruidException.forPersona(DruidException.Persona.USER)
                            .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                            .build(e, "Exception occurred while connecting to export destination.");
      }


      final ResultFormat resultFormat = exportMSQDestination.getResultFormat();
      final QueryDefinitionBuilder builder = QueryDefinition.builder();
      builder.addAll(queryDef);
      builder.add(StageDefinition.builder(queryDef.getNextStageNumber())
                                 .inputs(new StageInputSpec(queryDef.getFinalStageDefinition().getStageNumber()))
                                 .maxWorkerCount(tuningConfig.getMaxNumWorkers())
                                 .signature(queryDef.getFinalStageDefinition().getSignature())
                                 .shuffleSpec(null)
                                 .processorFactory(new ExportResultsFrameProcessorFactory(
                                     queryId,
                                     exportStorageProvider,
                                     resultFormat,
                                     querySpec.getColumnMappings()
                                 ))
      );
      return builder.build();
    } else if (querySpec.getDestination() instanceof DurableStorageMSQDestination) {
      return ControllerUtil.queryDefinitionForDurableStorage(queryDef, tuningConfig);
    } else if (querySpec.getDestination() instanceof TaskReportMSQDestination) {
      return queryDef;
    } else {
      throw new ISE("Unsupported destination [%s]", querySpec.getDestination());
    }
  }

  @Nullable
  private Yielder<Object[]> getFinalResultsYielder(
      final QueryDefinition queryDef,
      final ControllerQueryKernel queryKernel,
      final ColumnMappings columnMappings
  )
  {
    if (queryKernel.isSuccess() && isInlineResults(task.getQuerySpec())) {
      final StageId finalStageId = queryKernel.getStageId(queryDef.getFinalStageDefinition().getStageNumber());
      final List<String> taskIds = getTaskIds();
      final Closer closer = Closer.create();

      final ListeningExecutorService resultReaderExec =
          MoreExecutors.listeningDecorator(Execs.singleThreaded("result-reader-%d"));
      closer.register(resultReaderExec::shutdownNow);

      final InputChannelFactory inputChannelFactory;

      if (isDurableStorageEnabled || MSQControllerTask.writeResultsToDurableStorage(task.getQuerySpec())) {
        inputChannelFactory = DurableStorageInputChannelFactory.createStandardImplementation(
            id(),
            MSQTasks.makeStorageConnector(
                context.injector()),
            closer,
            MSQControllerTask.writeResultsToDurableStorage(task.getQuerySpec())
        );
      } else {
        inputChannelFactory = new WorkerInputChannelFactory(netClient, () -> taskIds);
      }

      final InputChannels inputChannels = new InputChannelsImpl(
          queryDef,
          queryKernel.getResultPartitionsForStage(finalStageId),
          inputChannelFactory,
          () -> ArenaMemoryAllocator.createOnHeap(5_000_000),
          new FrameProcessorExecutor(resultReaderExec),
          null
      );

      return Yielders.each(
          Sequences.concat(ControllerUtil.createFrameChannelSequences(queryKernel, inputChannels, finalStageId).collect(
                       Collectors.toList()))
                   .flatMap(
                       frame ->
                           NativeStatementResourceHelper.getResultSequence(
                               queryDef.getFinalStageDefinition(),
                               frame,
                               columnMappings
                           )
                   )
                   .withBaggage(resultReaderExec::shutdownNow)
      );
    } else {
      return null;
    }
  }

  @Override
  public TaskStatus runTask(final Closer closer)
  {
    QueryDefinition queryDef = null;
    ControllerQueryKernel queryKernel = null;
    ListenableFuture<?> workerTaskRunnerFuture = null;
    CounterSnapshotsTree countersSnapshot = null;
    Yielder<Object[]> resultsYielder = null;
    Throwable exceptionEncountered = null;

    final TaskState taskStateForReport;
    final MSQErrorReport errorForReport;

    try {
      // Planning-related: convert the native query from MSQSpec into a multi-stage QueryDefinition.
      this.queryStartTime = DateTimes.nowUtc();
      queryDef = initializeQueryDefAndState(closer);

      final InputSpecSlicerFactory inputSpecSlicerFactory = makeInputSpecSlicerFactory(makeDataSegmentTimelineView());

      // Execution-related: run the multi-stage QueryDefinition.
      final Pair<ControllerQueryKernel, ListenableFuture<?>> queryRunResult =
          new RunQueryUntilDone(queryDef, inputSpecSlicerFactory, closer).run();

      queryKernel = Preconditions.checkNotNull(queryRunResult.lhs);
      workerTaskRunnerFuture = Preconditions.checkNotNull(queryRunResult.rhs);
      resultsYielder = getFinalResultsYielder(queryDef, queryKernel, task.getQuerySpec().getColumnMappings());

    }
    catch (Throwable e) {
      exceptionEncountered = e;
    }

    // Fetch final counters in separate try, in case runQueryUntilDone threw an exception.
    try {
      countersSnapshot = getFinalCountersSnapshot(queryKernel);
    }
    catch (Throwable e) {
      if (exceptionEncountered != null) {
        exceptionEncountered.addSuppressed(e);
      } else {
        exceptionEncountered = e;
      }
    }

    if (queryKernel != null && queryKernel.isSuccess() && exceptionEncountered == null) {
      taskStateForReport = TaskState.SUCCESS;
      errorForReport = null;
    } else {
      // Query failure. Generate an error report and log the error(s) we encountered.
      final String selfHost = MSQTasks.getHostFromSelfNode(selfDruidNode);
      final MSQErrorReport controllerError =
          exceptionEncountered != null
          ? MSQErrorReport.fromException(
              id(),
              selfHost,
              null,
              exceptionEncountered,
              task.getQuerySpec().getColumnMappings()
          )
          : null;
      MSQErrorReport workerError = workerErrorRef.get();

      taskStateForReport = TaskState.FAILED;
      errorForReport = MSQTasks.makeErrorReport(id(), selfHost, controllerError, workerError);

      // Log the errors we encountered.
      if (controllerError != null) {
        log.warn("Controller: %s", MSQTasks.errorReportToLogMessage(controllerError));
      }

      if (workerError != null) {
        log.warn("Worker: %s", MSQTasks.errorReportToLogMessage(workerError));
      }
    }

    MSQResultsReport resultsReport = null;
    if (queryKernel != null && queryKernel.isSuccess()) {
      // If successful, encourage the tasks to exit successfully.
      // get results before posting finish to the tasks.
      if (resultsYielder != null) {
        resultsReport = ControllerUtil.makeResultsTaskReport(
            queryDef,
            resultsYielder,
            task.getQuerySpec().getColumnMappings(),
            null,
            MultiStageQueryContext.getSelectDestination(task.getQuerySpec().getQuery().context())
        );
        try {
          resultsYielder.close();
        }
        catch (IOException e) {
          throw new RuntimeException("Unable to fetch results of various worker tasks successfully", e);
        }
      } else {
        resultsReport = null;
      }
      postFinishToAllTasks();
      workerTaskLauncher.stop(false);
    } else {
      // If not successful, cancel running tasks.
      if (workerTaskLauncher != null) {
        workerTaskLauncher.stop(true);
      }
    }

    // Wait for worker tasks to exit. Ignore their return status. At this point, we've done everything we need to do,
    // so we don't care about the task exit status.
    if (workerTaskRunnerFuture != null) {
      try {
        workerTaskRunnerFuture.get();
      }
      catch (Exception ignored) {
        // Suppress.
      }
    }

    boolean shouldWaitForSegmentLoad = MultiStageQueryContext.shouldWaitForSegmentLoad(task.getQuerySpec()
                                                                                           .getQuery()
                                                                                           .context());

    return finalizeTaskRunning(
        queryKernel,
        shouldWaitForSegmentLoad,
        queryDef,
        taskStateForReport,
        errorForReport,
        countersSnapshot,
        resultsReport
    );
  }

  private QueryDefinition initializeQueryDefAndState(final Closer closer)
  {
    final QueryContext queryContext = task.getQuerySpec().getQuery().context();
    if (isFaultToleranceEnabled) {
      if (!queryContext.containsKey(MultiStageQueryContext.CTX_DURABLE_SHUFFLE_STORAGE)) {
        // if context key not set, enable durableStorage automatically.
        isDurableStorageEnabled = true;
      } else {
        // if context key is set, and durableStorage is turned on.
        if (MultiStageQueryContext.isDurableStorageEnabled(queryContext)) {
          isDurableStorageEnabled = true;
        } else {
          throw new MSQException(
              UnknownFault.forMessage(
                  StringUtils.format(
                      "Context param[%s] cannot be explicitly set to false when context param[%s] is"
                      + " set to true. Either remove the context param[%s] or explicitly set it to true.",
                      MultiStageQueryContext.CTX_DURABLE_SHUFFLE_STORAGE,
                      MultiStageQueryContext.CTX_FAULT_TOLERANCE,
                      MultiStageQueryContext.CTX_DURABLE_SHUFFLE_STORAGE
                  )));
        }
      }
    } else {
      isDurableStorageEnabled = MultiStageQueryContext.isDurableStorageEnabled(queryContext);
    }

    log.debug("Task [%s] durable storage mode is set to %s.", task.getId(), isDurableStorageEnabled);
    log.debug("Task [%s] fault tolerance mode is set to %s.", task.getId(), isFaultToleranceEnabled);

    this.selfDruidNode = context.selfNode();
    context.registerController(this, closer);

    this.netClient = new ExceptionWrappingWorkerClient(context.taskClientFor(this));
    closer.register(netClient::close);

    final QueryDefinition queryDef = makeQueryDefinition(
        id(),
        makeQueryControllerToolKit(),
        task.getQuerySpec()
    );

    QueryValidator.validateQueryDef(queryDef);
    queryDefRef.set(queryDef);

    final long maxParseExceptions = task.getQuerySpec().getQuery().context().getLong(
        MSQWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED,
        MSQWarnings.DEFAULT_MAX_PARSE_EXCEPTIONS_ALLOWED
    );

    ImmutableMap.Builder<String, Object> taskContextOverridesBuilder = ImmutableMap.builder();
    taskContextOverridesBuilder
        .put(MultiStageQueryContext.CTX_DURABLE_SHUFFLE_STORAGE, isDurableStorageEnabled)
        .put(MSQWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED, maxParseExceptions);

    if (MSQControllerTask.writeResultsToDurableStorage(task.getQuerySpec())) {
      taskContextOverridesBuilder.put(
          MultiStageQueryContext.CTX_SELECT_DESTINATION,
          MSQSelectDestination.DURABLESTORAGE.getName()
      );
    } else {
      // we need not pass the value 'TaskReport' to the worker since the worker impl does not do anything in such a case.
      // but we are passing it anyway for completeness
      taskContextOverridesBuilder.put(
          MultiStageQueryContext.CTX_SELECT_DESTINATION,
          MSQSelectDestination.TASKREPORT.getName()
      );

    }


    this.workerTaskLauncher = new MSQWorkerTaskLauncher(
        id(),
        task.getDataSource(),
        context,
        (failedTask, fault) -> {
          if (isFaultToleranceEnabled && ControllerQueryKernel.isRetriableFault(fault)) {
            addToKernelManipulationQueue((kernel) -> addToRetryQueue(kernel, failedTask.getWorkerNumber(), fault));
          } else {
            throw new MSQException(fault);
          }
        },
        taskContextOverridesBuilder.build(),
        // 10 minutes +- 2 minutes jitter
        TimeUnit.SECONDS.toMillis(600 + ThreadLocalRandom.current().nextInt(-4, 5) * 30L)
    );

    this.faultsExceededChecker = new FaultsExceededChecker(
        ImmutableMap.of(CannotParseExternalDataFault.CODE, maxParseExceptions)
    );

    stageToStatsMergingMode = new HashMap<>();
    queryDef.getStageDefinitions().forEach(
        stageDefinition ->
            stageToStatsMergingMode.put(
                stageDefinition.getId().getStageNumber(),
                ControllerUtil.finalizeClusterStatisticsMergeMode(
                    stageDefinition,
                    MultiStageQueryContext.getClusterStatisticsMergeMode(queryContext)
                )
            )
    );
    this.workerMemoryParameters = WorkerMemoryParameters.createProductionInstanceForController(context.injector());
    this.workerSketchFetcher = new WorkerSketchFetcher(
        netClient,
        workerTaskLauncher,
        isFaultToleranceEnabled
    );
    closer.register(workerSketchFetcher::close);

    return queryDef;
  }


}
