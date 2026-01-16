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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.msq.counters.CounterSnapshotsTree;
import org.apache.druid.msq.indexing.LegacyMSQSpec;
import org.apache.druid.msq.indexing.MSQControllerTask;
import org.apache.druid.msq.indexing.MSQNativeControllerTask;
import org.apache.druid.msq.indexing.MSQTuningConfig;
import org.apache.druid.msq.indexing.destination.ExportMSQDestination;
import org.apache.druid.msq.indexing.error.CannotParseExternalDataFault;
import org.apache.druid.msq.indexing.error.FaultsExceededChecker;
import org.apache.druid.msq.indexing.error.MSQErrorReport;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.indexing.error.QueryNotSupportedFault;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.msq.input.InputSpecSlicerFactory;
import org.apache.druid.msq.input.stage.StageInputSpec;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.QueryDefinitionBuilder;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.kernel.controller.ControllerQueryKernel;
import org.apache.druid.msq.querykit.QueryKitSpec;
import org.apache.druid.msq.querykit.ShuffleSpecFactory;
import org.apache.druid.msq.querykit.results.ExportResultsStageProcessor;
import org.apache.druid.msq.querykit.results.QueryResultStageProcessor;
import org.apache.druid.msq.util.ControllerUtil;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.msq.util.NativeStatementResourceHelper;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContext;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.storage.ExportStorageProvider;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

public class NativeControllerImpl extends AbstractController<MSQNativeControllerTask>
{

  public NativeControllerImpl(
      final LegacyMSQSpec querySpec,
      final ControllerContext controllerContext,
      final QueryKitSpecFactory queryKitSpecFactory
  )
  {
    super(querySpec, controllerContext, queryKitSpecFactory);
  }

  @Override
  protected boolean isNativeQuery()
  {
    return true;
  }

  @SuppressWarnings("unchecked")
  private static QueryDefinition makeQueryDefinition(
      final QueryKitSpec queryKitSpec,
      final LegacyMSQSpec querySpec,
      final Query<?> query
  )
  {
    final MSQTuningConfig tuningConfig = querySpec.getTuningConfig();
    final ShuffleSpecFactory resultShuffleSpecFactory;

    resultShuffleSpecFactory =
        querySpec.getDestination()
                 .getShuffleSpecFactory(MultiStageQueryContext.getRowsPerPage(querySpec.getContext()));

    final QueryDefinition queryDef;

    try {
      queryDef = queryKitSpec.getQueryKit().makeQueryDefinition(
          queryKitSpec,
          query,
          resultShuffleSpecFactory,
          0
      );
    }
    catch (MSQException e) {
      // If the toolkit throws a MSQFault, don't wrap it in a more generic QueryNotSupportedFault
      throw e;
    }
    catch (Exception e) {
      throw new MSQException(e, QueryNotSupportedFault.builder().withErrorMessage(e.getMessage()).build());
    }

    if (MSQControllerTask.isExport(querySpec.getDestination())) {
      final ExportMSQDestination exportMSQDestination = (ExportMSQDestination) querySpec.getDestination();
      final ExportStorageProvider exportStorageProvider = exportMSQDestination.getExportStorageProvider();

      final ResultFormat resultFormat = exportMSQDestination.getResultFormat();
      final QueryDefinitionBuilder builder = QueryDefinition.builder(queryKitSpec.getQueryId());
      builder.addAll(queryDef);
      builder.add(StageDefinition.builder(queryDef.getNextStageNumber())
                                 .inputs(new StageInputSpec(queryDef.getFinalStageDefinition().getStageNumber()))
                                 .maxWorkerCount(tuningConfig.getMaxNumWorkers())
                                 .signature(queryDef.getFinalStageDefinition().getSignature())
                                 .shuffleSpec(null)
                                 .processor(new ExportResultsStageProcessor(
                                     queryKitSpec.getQueryId(),
                                     exportStorageProvider,
                                     resultFormat,
                                     querySpec.getColumnMappings(),
                                     null
                                 ))
      );
      return builder.build();
    } else if (MSQControllerTask.writeFinalStageResultsToDurableStorage(querySpec.getDestination())) {
      return queryDefinitionForDurableStorage(queryDef, tuningConfig, queryKitSpec);
    } else if (MSQControllerTask.writeFinalResultsToTaskReport(querySpec.getDestination())) {
      return queryDef;
    } else {
      throw new ISE("Unsupported destination [%s]", querySpec.getDestination());
    }
  }

  @Override
  public MSQTaskReportPayload runInternal(final QueryListener queryListener, final Closer closer)
  {
    QueryDefinition queryDef = null;
    ControllerQueryKernel queryKernel = null;
    ListenableFuture<?> workerTaskRunnerFuture = null;
    CounterSnapshotsTree countersSnapshot = null;
    Throwable exceptionEncountered = null;

    final TaskState taskStateForReport;
    final MSQErrorReport errorForReport;

    mainThreadId.set(Thread.currentThread().getId());

    try {
      // Planning-related: convert the native query from MSQSpec into a multi-stage QueryDefinition.
      this.queryStartTime = DateTimes.nowUtc();
      context.registerController(this, closer);

      queryDef = initializeQueryDefAndState();

      this.netClient = closer.register(new ExceptionWrappingWorkerClient(context.newWorkerClient()));
      this.workerSketchFetcher = new WorkerSketchFetcher(
          netClient,
          workerManager,
          queryKernelConfig.isFaultTolerant(),
          MultiStageQueryContext.getSketchEncoding(querySpec.getContext())
      );
      closer.register(workerSketchFetcher::close);

      final InputSpecSlicerFactory inputSpecSlicerFactory =
          makeInputSpecSlicerFactory(context.newTableInputSpecSlicer(workerManager));

      final Pair<ControllerQueryKernel, ListenableFuture<?>> queryRunResult =
          new RunQueryUntilDone(
              queryDef,
              queryKernelConfig,
              inputSpecSlicerFactory,
              queryListener,
              closer
          ).run();

      queryKernel = Preconditions.checkNotNull(queryRunResult.lhs);
      workerTaskRunnerFuture = Preconditions.checkNotNull(queryRunResult.rhs);
      handleQueryResults(queryDef, queryKernel);
    }
    catch (Throwable e) {
      exceptionEncountered = e;
    }

    // Fetch final counters in a separate try, in case runQueryUntilDone threw an exception.
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
      final MSQErrorReport controllerError;

      if (exceptionEncountered != null) {
        controllerError = MSQErrorReport.fromException(
            queryId(),
            selfHost,
            null,
            exceptionEncountered,
            querySpec.getColumnMappings()
        );
      } else {
        controllerError = null;
      }

      MSQErrorReport workerError = workerErrorRef.get();

      taskStateForReport = TaskState.FAILED;
      errorForReport = MSQTasks.makeErrorReport(queryId(), selfHost, controllerError, workerError);

      // Log the errors we encountered.
      if (controllerError != null) {
        log.warn("Controller: %s", MSQTasks.errorReportToLogMessage(controllerError));
      }

      if (workerError != null) {
        log.warn("Worker: %s", MSQTasks.errorReportToLogMessage(workerError));
      }
    }

    if (queryKernel != null && queryKernel.isSuccess()) {
      // If successful, encourage workers to exit successfully.
      // Only send this command to participating workers. For task-based queries, this is all tasks, since tasks
      // are launched only when needed. For Dart, this is any servers that were actually assigned work items.
      postFinishToWorkers(queryKernel.getAllParticipatingWorkers());
      workerManager.stop(false);
    } else {
      // If not successful, cancel running tasks.
      if (workerManager != null) {
        workerManager.stop(true);
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

    boolean shouldWaitForSegmentLoad = MultiStageQueryContext.shouldWaitForSegmentLoad(querySpec.getContext());

    return finalizeTaskRunning(
        queryKernel,
        shouldWaitForSegmentLoad,
        queryDef,
        taskStateForReport,
        errorForReport,
        countersSnapshot
    );
  }

  @Override
  protected ControllerQueryResultsReader instantiateResultsReader(
      ReadableFrameChannel in,
      FrameReader frameReader,
      ColumnMappings columnMappings,
      QueryListener queryListener
  )
  {
    return new ControllerQueryResultsReader(
        in,
        frameReader,
        columnMappings,
        null,
        null,
        queryListener,
        NativeStatementResourceHelper.INSTANCE
    );
  }

  private QueryDefinition initializeQueryDefAndState()
  {
    this.selfDruidNode = context.selfNode();
    this.queryKernelConfig = context.queryKernelConfig(querySpec);

    final QueryContext queryContext = querySpec.getContext();
    assert legacyQuery != null : "legacyQuery is null"; // should not happen
    final QueryDefinition queryDef = makeQueryDefinition(
        queryKitSpecFactory.makeQueryKitSpec(
            makeQueryControllerToolKit(queryContext),
            queryId(),
            querySpec.getTuningConfig(),
            queryContext
        ),
        (LegacyMSQSpec) querySpec,
        legacyQuery
    );

    ensureExportLocationEmpty(context, querySpec.getDestination());

    if (log.isDebugEnabled()) {
      try {
        log.debug(
            "Query[%s] definition: %s",
            queryDef.getQueryId(),
            context.jsonMapper().writerWithDefaultPrettyPrinter().writeValueAsString(queryDef)
        );
      }
      catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }

    QueryValidator.validateQueryDef(queryDef);
    queryDefRef.set(queryDef);
    workerManager = initWorkerManager();

    if (queryKernelConfig.isFaultTolerant() && !(workerManager instanceof RetryCapableWorkerManager)) {
      // Not expected to happen, since all WorkerManager impls are currently retry-capable. Defensive check
      // for future-proofing.
      throw DruidException.defensive(
          "Cannot run with fault tolerance since workerManager class[%s] does not support retrying",
          workerManager.getClass().getName()
      );
    }

    final long maxParseExceptions = MultiStageQueryContext.getMaxParseExceptions(queryContext);

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

    return queryDef;
  }

  private void handleQueryResults(
      final QueryDefinition queryDef,
      final ControllerQueryKernel queryKernel
  ) throws IOException
  {
    if (!queryKernel.isSuccess()) {
      return;
    }
    if (MSQControllerTask.isExport(querySpec.getDestination())) {
      // Write manifest file.
      ExportMSQDestination destination = (ExportMSQDestination) querySpec.getDestination();
      ExportMetadataManager exportMetadataManager = new ExportMetadataManager(
          destination.getExportStorageProvider(),
          context.taskTempDir()
      );
      final StageId finalStageId = queryKernel.getStageId(queryDef.getFinalStageDefinition().getStageNumber());
      //noinspection unchecked

      Object resultObjectForStage = queryKernel.getResultObjectForStage(finalStageId);
      if (!(resultObjectForStage instanceof List)) {
        // This might occur if all workers are running on an older version. We are not able to write a manifest file in this case.
        log.warn(
            "Unable to create export manifest file. Received result[%s] from worker instead of a list of file names.",
            resultObjectForStage
        );
        return;
      }
      @SuppressWarnings("unchecked")
      List<String> exportedFiles = (List<String>) queryKernel.getResultObjectForStage(finalStageId);
      log.info("Query [%s] exported %d files.", queryDef.getQueryId(), exportedFiles.size());
      exportMetadataManager.writeMetadata(exportedFiles);
    }
  }

  private static QueryDefinition queryDefinitionForDurableStorage(
      final QueryDefinition queryDef,
      final MSQTuningConfig tuningConfig,
      final QueryKitSpec queryKitSpec
  )
  {
    // attaching new query results stage if the final stage does sort during shuffle so that results are ordered.
    StageDefinition finalShuffleStageDef = queryDef.getFinalStageDefinition();
    if (finalShuffleStageDef.doesSortDuringShuffle()) {
      final QueryDefinitionBuilder builder = QueryDefinition.builder(queryKitSpec.getQueryId());
      builder.addAll(queryDef);
      builder.add(StageDefinition.builder(queryDef.getNextStageNumber())
                                 .inputs(new StageInputSpec(queryDef.getFinalStageDefinition().getStageNumber()))
                                 .maxWorkerCount(tuningConfig.getMaxNumWorkers())
                                 .signature(finalShuffleStageDef.getSignature())
                                 .shuffleSpec(null)
                                 .processor(new QueryResultStageProcessor())
      );
      return builder.build();
    } else {
      return queryDef;
    }
  }
}
