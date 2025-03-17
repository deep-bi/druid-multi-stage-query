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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.druid.client.indexing.ClientCompactionTaskTransformSpec;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.actions.MarkSegmentsAsUnusedAction;
import org.apache.druid.indexing.common.actions.SegmentTransactionalAppendAction;
import org.apache.druid.indexing.common.actions.SegmentTransactionalInsertAction;
import org.apache.druid.indexing.common.actions.SegmentTransactionalReplaceAction;
import org.apache.druid.indexing.common.actions.TaskAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.common.task.batch.TooManyBucketsException;
import org.apache.druid.indexing.common.task.batch.parallel.TombstoneHelper;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.counters.CounterSnapshotsTree;
import org.apache.druid.msq.indexing.MSQControllerTask;
import org.apache.druid.msq.indexing.MSQSpec;
import org.apache.druid.msq.indexing.MSQTuningConfig;
import org.apache.druid.msq.indexing.destination.DataSourceMSQDestination;
import org.apache.druid.msq.indexing.destination.ExportMSQDestination;
import org.apache.druid.msq.indexing.error.CannotParseExternalDataFault;
import org.apache.druid.msq.indexing.error.FaultsExceededChecker;
import org.apache.druid.msq.indexing.error.InsertLockPreemptedFault;
import org.apache.druid.msq.indexing.error.MSQErrorReport;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.indexing.error.QueryNotSupportedFault;
import org.apache.druid.msq.indexing.error.TooManyBucketsFault;
import org.apache.druid.msq.indexing.error.UnknownFault;
import org.apache.druid.msq.indexing.processor.SegmentGeneratorFrameProcessorFactory;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.msq.input.InputSpecSlicerFactory;
import org.apache.druid.msq.input.InputSpecs;
import org.apache.druid.msq.input.stage.StageInputSpec;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.QueryDefinitionBuilder;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.kernel.controller.ControllerQueryKernel;
import org.apache.druid.msq.querykit.QueryKitSpec;
import org.apache.druid.msq.querykit.QueryKitUtils;
import org.apache.druid.msq.querykit.ShuffleSpecFactory;
import org.apache.druid.msq.querykit.results.ExportResultsFrameProcessorFactory;
import org.apache.druid.msq.util.ArrayIngestMode;
import org.apache.druid.msq.util.ControllerUtil;
import org.apache.druid.msq.util.DimensionSchemaUtils;
import org.apache.druid.msq.util.IntervalUtils;
import org.apache.druid.msq.util.MSQTaskQueryMakerUtils;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.msq.util.PassthroughAggregatorFactory;
import org.apache.druid.msq.util.SqlStatementResourceHelper;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.sql.calcite.parser.DruidSqlInsert;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.storage.ExportStorageProvider;
import org.apache.druid.timeline.CompactionState;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.DimensionRangeShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;
import org.apache.druid.utils.CollectionUtils;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ControllerImpl extends AbstractController<MSQControllerTask>
{
  private static final Logger log = new Logger(ControllerImpl.class);
  protected final ResultsContext resultsContext;


  public ControllerImpl(
      final String queryId,
      final MSQSpec querySpec,
      final ResultsContext resultsContext,
      final ControllerContext controllerContext
  )
  {
    super(queryId, querySpec, controllerContext);
    this.resultsContext = Preconditions.checkNotNull(resultsContext, "resultsContext");
  }

  private static TaskAction<SegmentPublishResult> createAppendAction(
      Set<DataSegment> segments,
      TaskLockType taskLockType
  )
  {
    if (taskLockType.equals(TaskLockType.APPEND)) {
      return SegmentTransactionalAppendAction.forSegments(segments, null);
    } else if (taskLockType.equals(TaskLockType.SHARED)) {
      return SegmentTransactionalInsertAction.appendAction(segments, null, null, null);
    } else {
      throw DruidException.defensive("Invalid lock type [%s] received for append action", taskLockType);
    }
  }

  private static Pair<List<DimensionSchema>, List<AggregatorFactory>> makeDimensionsAndAggregatorsForIngestion(
      final RowSignature querySignature,
      final ClusterBy queryClusterBy,
      final List<String> segmentSortOrder,
      final ColumnMappings columnMappings,
      final boolean isRollupQuery,
      final Query<?> query
  )
  {
    // Log a warning unconditionally if arrayIngestMode is MVD, since the behaviour is incorrect, and is subject to
    // deprecation and removal in future
    if (MultiStageQueryContext.getArrayIngestMode(query.context()) == ArrayIngestMode.MVD) {
      log.warn(
          "'%s' is set to 'mvd' in the query's context. This ingests the string arrays as multi-value "
          + "strings instead of arrays, and is preserved for legacy reasons when MVDs were the only way to ingest string "
          + "arrays in Druid. It is incorrect behaviour and will likely be removed in the future releases of Druid",
          MultiStageQueryContext.CTX_ARRAY_INGEST_MODE
      );
    }

    final List<DimensionSchema> dimensions = new ArrayList<>();
    final List<AggregatorFactory> aggregators = new ArrayList<>();

    // During ingestion, segment sort order is determined by the order of fields in the DimensionsSchema. We want
    // this to match user intent as dictated by the declared segment sort order and CLUSTERED BY, so add things in
    // that order.

    // Start with segmentSortOrder.
    final Set<String> outputColumnsInOrder = new LinkedHashSet<>(segmentSortOrder);

    // Then the query-level CLUSTERED BY.
    // Note: this doesn't work when CLUSTERED BY specifies an expression that is not being selected.
    // Such fields in CLUSTERED BY still control partitioning as expected, but do not affect sort order of rows
    // within an individual segment.
    for (final KeyColumn clusterByColumn : queryClusterBy.getColumns()) {
      final IntList outputColumns = columnMappings.getOutputColumnsForQueryColumn(clusterByColumn.columnName());
      for (final int outputColumn : outputColumns) {
        outputColumnsInOrder.add(columnMappings.getOutputColumnName(outputColumn));
      }
    }

    // Then all other columns.
    outputColumnsInOrder.addAll(columnMappings.getOutputColumnNames());

    Map<String, AggregatorFactory> outputColumnAggregatorFactories = new HashMap<>();

    if (isRollupQuery) {
      // Populate aggregators from the native query when doing an ingest in rollup mode.
      for (AggregatorFactory aggregatorFactory : ((GroupByQuery) query).getAggregatorSpecs()) {
        for (final int outputColumn : columnMappings.getOutputColumnsForQueryColumn(aggregatorFactory.getName())) {
          final String outputColumnName = columnMappings.getOutputColumnName(outputColumn);
          if (outputColumnAggregatorFactories.containsKey(outputColumnName)) {
            throw new ISE("There can only be one aggregation for column [%s].", outputColumn);
          } else {
            outputColumnAggregatorFactories.put(
                outputColumnName,
                aggregatorFactory.withName(outputColumnName).getCombiningFactory()
            );
          }
        }
      }
    }

    // Each column can be of either time, dimension, aggregator. For this method. we can ignore the time column.
    // For non-complex columns, If the aggregator factory of the column is not available, we treat the column as
    // a dimension. For complex columns, certains hacks are in place.
    for (final String outputColumnName : outputColumnsInOrder) {
      // CollectionUtils.getOnlyElement because this method is only called during ingestion, where we require
      // that output names be unique.
      final int outputColumn = CollectionUtils.getOnlyElement(
          columnMappings.getOutputColumnsByName(outputColumnName),
          xs -> new ISE("Expected single output column for name [%s], but got [%s]", outputColumnName, xs)
      );
      final String queryColumn = columnMappings.getQueryColumnName(outputColumn);
      final ColumnType type =
          querySignature.getColumnType(queryColumn)
                        .orElseThrow(() -> new ISE("No type for column [%s]", outputColumnName));

      if (!outputColumnName.equals(ColumnHolder.TIME_COLUMN_NAME)) {

        if (!type.is(ValueType.COMPLEX)) {
          // non complex columns
          populateDimensionsAndAggregators(
              dimensions,
              aggregators,
              outputColumnAggregatorFactories,
              outputColumnName,
              type,
              query.context()
          );
        } else {
          // complex columns only
          if (DimensionHandlerUtils.DIMENSION_HANDLER_PROVIDERS.containsKey(type.getComplexTypeName())) {
            dimensions.add(
                DimensionSchemaUtils.createDimensionSchema(
                    outputColumnName,
                    type,
                    MultiStageQueryContext.useAutoColumnSchemas(query.context()),
                    MultiStageQueryContext.getArrayIngestMode(query.context())
                )
            );
          } else if (!isRollupQuery) {
            aggregators.add(new PassthroughAggregatorFactory(outputColumnName, type.getComplexTypeName()));
          } else {
            populateDimensionsAndAggregators(
                dimensions,
                aggregators,
                outputColumnAggregatorFactories,
                outputColumnName,
                type,
                query.context()
            );
          }
        }
      }
    }

    return Pair.of(dimensions, aggregators);
  }

  /**
   * If the output column is present in the outputColumnAggregatorFactories that means we already have the aggregator information for this column.
   * else treat this column as a dimension.
   *
   * @param dimensions                      list is poulated if the output col is deemed to be a dimension
   * @param aggregators                     list is populated with the aggregator if the output col is deemed to be a aggregation column.
   * @param outputColumnAggregatorFactories output col -> AggregatorFactory map
   * @param outputColumn                    column name
   * @param type                            columnType
   */
  private static void populateDimensionsAndAggregators(
      List<DimensionSchema> dimensions,
      List<AggregatorFactory> aggregators,
      Map<String, AggregatorFactory> outputColumnAggregatorFactories,
      String outputColumn,
      ColumnType type,
      QueryContext context
  )
  {
    if (outputColumnAggregatorFactories.containsKey(outputColumn)) {
      aggregators.add(outputColumnAggregatorFactories.get(outputColumn));
    } else {
      dimensions.add(
          DimensionSchemaUtils.createDimensionSchema(
              outputColumn,
              type,
              MultiStageQueryContext.useAutoColumnSchemas(context),
              MultiStageQueryContext.getArrayIngestMode(context)
          )
      );
    }
  }

  /**
   * Performs a particular {@link SegmentTransactionalInsertAction}, publishing segments.
   * <p>
   * Throws {@link MSQException} with {@link InsertLockPreemptedFault} if the action fails due to lock preemption.
   */
  static void performSegmentPublish(
      final TaskActionClient client,
      final TaskAction<SegmentPublishResult> action
  ) throws IOException
  {
    try {
      final SegmentPublishResult result = client.submit(action);

      if (!result.isSuccess()) {
        throw new MSQException(InsertLockPreemptedFault.instance());
      }
    }
    catch (Exception e) {
      if (isTaskLockPreemptedException(e)) {
        throw new MSQException(e, InsertLockPreemptedFault.instance());
      } else {
        throw e;
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static QueryDefinition makeQueryDefinition(
      final QueryKitSpec queryKitSpec,
      final MSQSpec querySpec,
      final ObjectMapper jsonMapper,
      final ResultsContext resultsContext
  )
  {
    final MSQTuningConfig tuningConfig = querySpec.getTuningConfig();
    final ColumnMappings columnMappings = querySpec.getColumnMappings();
    final Query<?> queryToPlan;
    final ShuffleSpecFactory resultShuffleSpecFactory;

    if (MSQControllerTask.isIngestion(querySpec)) {
      resultShuffleSpecFactory = querySpec.getDestination()
                                          .getShuffleSpecFactory(tuningConfig.getRowsPerSegment());

      if (!columnMappings.hasUniqueOutputColumnNames()) {
        // We do not expect to hit this case in production, because the SQL validator checks that column names
        // are unique for INSERT and REPLACE statements (i.e. anything where MSQControllerTask.isIngestion would
        // be true). This check is here as defensive programming.
        throw new ISE("Column names are not unique: [%s]", columnMappings.getOutputColumnNames());
      }

      MSQTaskQueryMakerUtils.validateRealtimeReindex(querySpec);

      if (columnMappings.hasOutputColumn(ColumnHolder.TIME_COLUMN_NAME)) {
        // We know there's a single time column, because we've checked columnMappings.hasUniqueOutputColumnNames().
        final int timeColumn = columnMappings.getOutputColumnsByName(ColumnHolder.TIME_COLUMN_NAME).getInt(0);
        queryToPlan = querySpec.getQuery().withOverriddenContext(
            ImmutableMap.of(
                QueryKitUtils.CTX_TIME_COLUMN_NAME,
                columnMappings.getQueryColumnName(timeColumn)
            )
        );
      } else {
        queryToPlan = querySpec.getQuery();
      }
    } else {
      resultShuffleSpecFactory =
          querySpec.getDestination()
                   .getShuffleSpecFactory(MultiStageQueryContext.getRowsPerPage(querySpec.getQuery().context()));
      queryToPlan = querySpec.getQuery();
    }

    final QueryDefinition queryDef;

    try {
      queryDef = queryKitSpec.getQueryKit().makeQueryDefinition(
          queryKitSpec,
          queryToPlan,
          resultShuffleSpecFactory,
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

    if (MSQControllerTask.isIngestion(querySpec)) {
      // Find the stage that provides shuffled input to the final segment-generation stage.
      StageDefinition finalShuffleStageDef = queryDef.getFinalStageDefinition();

      while (!finalShuffleStageDef.doesShuffle()
             && InputSpecs.getStageNumbers(finalShuffleStageDef.getInputSpecs()).size() == 1) {
        finalShuffleStageDef = queryDef.getStageDefinition(
            Iterables.getOnlyElement(InputSpecs.getStageNumbers(finalShuffleStageDef.getInputSpecs()))
        );
      }

      if (!finalShuffleStageDef.doesShuffle()) {
        finalShuffleStageDef = null;
      }

      // Add all query stages.
      // Set shuffleCheckHasMultipleValues on the stage that serves as input to the final segment-generation stage.
      final QueryDefinitionBuilder builder = QueryDefinition.builder(queryKitSpec.getQueryId());

      for (final StageDefinition stageDef : queryDef.getStageDefinitions()) {
        if (stageDef.equals(finalShuffleStageDef)) {
          builder.add(StageDefinition.builder(stageDef).shuffleCheckHasMultipleValues(true));
        } else {
          builder.add(StageDefinition.builder(stageDef));
        }
      }

      // Then, add a segment-generation stage.
      final DataSourceMSQDestination destination = (DataSourceMSQDestination) querySpec.getDestination();

      return builder.add(
                        destination.getTerminalStageSpec()
                                   .constructFinalStage(
                                       queryDef,
                                       querySpec,
                                       jsonMapper
                                   )
                    )
                    .build();
    } else if (MSQControllerTask.writeFinalResultsToTaskReport(querySpec)) {
      return queryDef;
    } else if (MSQControllerTask.writeFinalStageResultsToDurableStorage(querySpec)) {
      return ControllerUtil.queryDefinitionForDurableStorage(queryDef, tuningConfig, queryKitSpec);
    } else if (MSQControllerTask.isExport(querySpec)) {
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
      final QueryDefinitionBuilder builder = QueryDefinition.builder(queryKitSpec.getQueryId());
      builder.addAll(queryDef);
      builder.add(StageDefinition.builder(queryDef.getNextStageNumber())
                                 .inputs(new StageInputSpec(queryDef.getFinalStageDefinition().getStageNumber()))
                                 .maxWorkerCount(tuningConfig.getMaxNumWorkers())
                                 .signature(queryDef.getFinalStageDefinition().getSignature())
                                 .shuffleSpec(null)
                                 .processorFactory(new ExportResultsFrameProcessorFactory(
                                     queryKitSpec.getQueryId(),
                                     exportStorageProvider,
                                     resultFormat,
                                     columnMappings,
                                     resultsContext
                                 ))
      );
      return builder.build();
    } else {
      throw new ISE("Unsupported destination [%s]", querySpec.getDestination());
    }
  }

  private static Function<Set<DataSegment>, Set<DataSegment>> addCompactionStateToSegments(
      MSQSpec querySpec,
      ObjectMapper jsonMapper,
      DataSchema dataSchema,
      @Nullable ShardSpec shardSpec,
      @Nullable ClusterBy clusterBy,
      String queryId
  )
  {
    final MSQTuningConfig tuningConfig = querySpec.getTuningConfig();
    PartitionsSpec partitionSpec;

    // shardSpec is absent in the absence of segments, which happens when only tombstones are generated by an
    // MSQControllerTask.
    if (shardSpec != null) {
      if (Objects.equals(shardSpec.getType(), ShardSpec.Type.RANGE)) {
        List<String> partitionDimensions = ((DimensionRangeShardSpec) shardSpec).getDimensions();
        // Effective maxRowsPerSegment is propagated as rowsPerSegment in MSQ
        partitionSpec = new DimensionRangePartitionsSpec(
            null,
            tuningConfig.getRowsPerSegment(),
            partitionDimensions,
            false
        );
      } else if (Objects.equals(shardSpec.getType(), ShardSpec.Type.NUMBERED)) {
        // MSQ tasks don't use maxTotalRows. Hence using LONG.MAX_VALUE.
        partitionSpec = new DynamicPartitionsSpec(tuningConfig.getRowsPerSegment(), Long.MAX_VALUE);
      } else {
        // SingleDimenionShardSpec and other shard specs are never created in MSQ.
        throw new MSQException(
            UnknownFault.forMessage(
                StringUtils.format(
                    "Query[%s] cannot store compaction state in segments as shard spec of unsupported type[%s].",
                    queryId,
                    shardSpec.getType()
                )));
      }
    } else if (clusterBy != null && !clusterBy.getColumns().isEmpty()) {
      // Effective maxRowsPerSegment is propagated as rowsPerSegment in MSQ
      partitionSpec = new DimensionRangePartitionsSpec(
          null,
          tuningConfig.getRowsPerSegment(),
          clusterBy.getColumns()
                   .stream()
                   .map(KeyColumn::columnName).collect(Collectors.toList()),
          false
      );
    } else {
      partitionSpec = new DynamicPartitionsSpec(tuningConfig.getRowsPerSegment(), Long.MAX_VALUE);
    }

    Granularity segmentGranularity = ((DataSourceMSQDestination) querySpec.getDestination())
        .getSegmentGranularity();

    GranularitySpec granularitySpec = new UniformGranularitySpec(
        segmentGranularity,
        QueryContext.of(querySpec.getQuery().getContext())
                    .getGranularity(DruidSqlInsert.SQL_INSERT_QUERY_GRANULARITY, jsonMapper),
        dataSchema.getGranularitySpec().isRollup(),
        // Not using dataSchema.getGranularitySpec().inputIntervals() as that always has ETERNITY
        ((DataSourceMSQDestination) querySpec.getDestination()).getReplaceTimeChunks()
    );

    DimensionsSpec dimensionsSpec = dataSchema.getDimensionsSpec();
    Map<String, Object> transformSpec = TransformSpec.NONE.equals(dataSchema.getTransformSpec())
                                        ? null
                                        : new ClientCompactionTaskTransformSpec(
                                            dataSchema.getTransformSpec().getFilter()
                                        ).asMap(jsonMapper);
    List<Object> metricsSpec = Collections.emptyList();

    if (querySpec.getQuery() instanceof GroupByQuery) {
      // For group-by queries, the aggregators are transformed to their combining factories in the dataschema, resulting
      // in a mismatch between schema in compaction spec and the one in compaction state. Sourcing the original
      // AggregatorFactory definition for aggregators in the dataSchema, therefore, directly from the querySpec.
      GroupByQuery groupByQuery = (GroupByQuery) querySpec.getQuery();
      // Collect all aggregators that are part of the current dataSchema, since a non-rollup query (isRollup() is false)
      // moves metrics columns to dimensions in the final schema.
      Set<String> aggregatorsInDataSchema = Arrays.stream(dataSchema.getAggregators())
                                                  .map(AggregatorFactory::getName)
                                                  .collect(
                                                      Collectors.toSet());
      metricsSpec = new ArrayList<>(
          groupByQuery.getAggregatorSpecs()
                      .stream()
                      .filter(aggregatorFactory -> aggregatorsInDataSchema.contains(aggregatorFactory.getName()))
                      .collect(Collectors.toList())
      );
    }

    IndexSpec indexSpec = tuningConfig.getIndexSpec();

    log.info("Query[%s] storing compaction state in segments.", queryId);

    return CompactionState.addCompactionStateToSegments(
        partitionSpec,
        dimensionsSpec,
        metricsSpec,
        transformSpec,
        indexSpec.asMap(jsonMapper),
        granularitySpec.asMap(jsonMapper)
    );
  }

  @Override
  protected boolean isNativeQuery()
  {
    return false;
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

    try {
      // Planning-related: convert the native query from MSQSpec into a multi-stage QueryDefinition.
      this.queryStartTime = DateTimes.nowUtc();
      context.registerController(this, closer);

      queryDef = initializeQueryDefAndState(closer);

      // Execution-related: run the multi-stage QueryDefinition.
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

    boolean shouldWaitForSegmentLoad = MultiStageQueryContext.shouldWaitForSegmentLoad(querySpec.getQuery().context());


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
        resultsContext,
        context.jsonMapper(),
        queryListener,
        SqlStatementResourceHelper.INSTANCE
    );
  }

  private QueryDefinition initializeQueryDefAndState(final Closer closer)
  {
    this.selfDruidNode = context.selfNode();
    this.netClient = closer.register(new ExceptionWrappingWorkerClient(context.newWorkerClient()));
    this.queryKernelConfig = context.queryKernelConfig(queryId, querySpec);

    final QueryContext queryContext = querySpec.getQuery().context();
    final QueryDefinition queryDef = makeQueryDefinition(
        context.makeQueryKitSpec(makeQueryControllerToolKit(), queryId, querySpec, queryKernelConfig),
        querySpec,
        context.jsonMapper(),
        resultsContext
    );

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
    this.workerSketchFetcher = new WorkerSketchFetcher(
        netClient,
        workerManager,
        queryKernelConfig.isFaultTolerant(),
        MultiStageQueryContext.getSketchEncoding(querySpec.getQuery().context())
    );
    closer.register(workerSketchFetcher::close);

    return queryDef;
  }

  /**
   * Publish the list of segments. Additionally, if {@link DataSourceMSQDestination#isReplaceTimeChunks()},
   * also drop all other segments within the replacement intervals.
   */
  private void publishAllSegments(
      final Set<DataSegment> segments,
      Function<Set<DataSegment>, Set<DataSegment>> compactionStateAnnotateFunction
  ) throws IOException
  {
    final DataSourceMSQDestination destination =
        (DataSourceMSQDestination) querySpec.getDestination();
    final Set<DataSegment> segmentsWithTombstones = new HashSet<>(segments);
    int numTombstones = 0;
    final TaskLockType taskLockType = context.taskLockType();

    if (destination.isReplaceTimeChunks()) {
      final List<Interval> intervalsToDrop = findIntervalsToDrop(Preconditions.checkNotNull(segments, "segments"));

      if (!intervalsToDrop.isEmpty()) {
        TombstoneHelper tombstoneHelper = new TombstoneHelper(context.taskActionClient());
        try {
          Set<DataSegment> tombstones = tombstoneHelper.computeTombstoneSegmentsForReplace(
              intervalsToDrop,
              destination.getReplaceTimeChunks(),
              destination.getDataSource(),
              destination.getSegmentGranularity(),
              Limits.MAX_PARTITION_BUCKETS
          );
          segmentsWithTombstones.addAll(tombstones);
          numTombstones = tombstones.size();
        }
        catch (IllegalStateException e) {
          throw new MSQException(e, InsertLockPreemptedFault.instance());
        }
        catch (TooManyBucketsException e) {
          throw new MSQException(e, new TooManyBucketsFault(Limits.MAX_PARTITION_BUCKETS));
        }
      }

      if (segmentsWithTombstones.isEmpty()) {
        // Nothing to publish, only drop. We already validated that the intervalsToDrop do not have any
        // partially-overlapping segments, so it's safe to drop them as intervals instead of as specific segments.
        // This should not need a segment load wait as segments are marked as unused immediately.
        for (final Interval interval : intervalsToDrop) {
          context.taskActionClient()
                 .submit(new MarkSegmentsAsUnusedAction(destination.getDataSource(), interval));
        }
      } else {
        if (MultiStageQueryContext.shouldWaitForSegmentLoad(querySpec.getQuery().context())) {
          initLoadWaiter(segmentsWithTombstones, destination);
        }
        performSegmentPublish(
            context.taskActionClient(),
            createOverwriteAction(taskLockType, compactionStateAnnotateFunction.apply(segmentsWithTombstones))
        );
      }
    } else if (!segments.isEmpty()) {
      if (MultiStageQueryContext.shouldWaitForSegmentLoad(querySpec.getQuery().context())) {
        initLoadWaiter(segments, destination);
      }
      // Append mode.
      performSegmentPublish(
          context.taskActionClient(),
          createAppendAction(segments, taskLockType)
      );
    }

    context.emitMetric("ingest/tombstones/count", numTombstones);
    // Include tombstones in the reported segments count
    context.emitMetric("ingest/segments/count", segmentsWithTombstones.size());
  }

  private TaskAction<SegmentPublishResult> createOverwriteAction(
      TaskLockType taskLockType,
      Set<DataSegment> segmentsWithTombstones
  )
  {
    if (taskLockType.equals(TaskLockType.REPLACE)) {
      return SegmentTransactionalReplaceAction.create(segmentsWithTombstones, null);
    } else if (taskLockType.equals(TaskLockType.EXCLUSIVE)) {
      return SegmentTransactionalInsertAction.overwriteAction(null, segmentsWithTombstones, null);
    } else {
      throw DruidException.defensive("Invalid lock type [%s] received for overwrite action", taskLockType);
    }
  }

  /**
   * When doing an ingestion with {@link DataSourceMSQDestination#isReplaceTimeChunks()}, finds intervals
   * containing data that should be dropped.
   */
  private List<Interval> findIntervalsToDrop(final Set<DataSegment> publishedSegments)
  {
    // Safe to cast because publishAllSegments is only called for dataSource destinations.
    final DataSourceMSQDestination destination =
        (DataSourceMSQDestination) querySpec.getDestination();
    final List<Interval> replaceIntervals =
        new ArrayList<>(JodaUtils.condenseIntervals(destination.getReplaceTimeChunks()));
    final List<Interval> publishIntervals =
        JodaUtils.condenseIntervals(Iterables.transform(publishedSegments, DataSegment::getInterval));
    return IntervalUtils.difference(replaceIntervals, publishIntervals);
  }

  private void handleQueryResults(
      final QueryDefinition queryDef,
      final ControllerQueryKernel queryKernel
  ) throws IOException
  {
    if (!queryKernel.isSuccess()) {
      return;
    }
    if (MSQControllerTask.isIngestion(querySpec)) {
      // Publish segments if needed.
      final StageId finalStageId = queryKernel.getStageId(queryDef.getFinalStageDefinition().getStageNumber());
      Function<Set<DataSegment>, Set<DataSegment>> compactionStateAnnotateFunction = Function.identity();

      //noinspection unchecked
      @SuppressWarnings("unchecked")
      Set<DataSegment> segments = (Set<DataSegment>) queryKernel.getResultObjectForStage(finalStageId);

      boolean storeCompactionState = QueryContext.of(querySpec.getQuery().getContext())
                                                 .getBoolean(
                                                     Tasks.STORE_COMPACTION_STATE_KEY,
                                                     Tasks.DEFAULT_STORE_COMPACTION_STATE
                                                 );

      if (storeCompactionState) {
        DataSourceMSQDestination destination = (DataSourceMSQDestination) querySpec.getDestination();
        if (!destination.isReplaceTimeChunks()) {
          // Store compaction state only for replace queries.
          log.warn(
              "Ignoring storeCompactionState flag since it is set for a non-REPLACE query[%s].",
              queryDef.getQueryId()
          );
        } else {
          DataSchema dataSchema = ((SegmentGeneratorFrameProcessorFactory) queryKernel
              .getStageDefinition(finalStageId).getProcessorFactory()).getDataSchema();

          ShardSpec shardSpec = segments.isEmpty() ? null : segments.stream().findFirst().get().getShardSpec();
          ClusterBy clusterBy = queryKernel.getStageDefinition(finalStageId).getClusterBy();

          compactionStateAnnotateFunction = addCompactionStateToSegments(
              querySpec,
              context.jsonMapper(),
              dataSchema,
              shardSpec,
              clusterBy,
              queryDef.getQueryId()
          );
        }
      }
      log.info("Query [%s] publishing %d segments.", queryDef.getQueryId(), segments.size());
      publishAllSegments(segments, compactionStateAnnotateFunction);
    } else if (MSQControllerTask.isExport(querySpec)) {
      // Write manifest file.
      ExportMSQDestination destination = (ExportMSQDestination) querySpec.getDestination();
      ExportMetadataManager exportMetadataManager = new ExportMetadataManager(destination.getExportStorageProvider());

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
}
