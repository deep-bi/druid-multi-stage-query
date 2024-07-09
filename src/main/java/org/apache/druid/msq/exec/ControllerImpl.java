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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.discovery.BrokerClient;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.allocation.ArenaMemoryAllocator;
import org.apache.druid.frame.channel.FrameChannelSequence;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.processor.FrameProcessorExecutor;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.indexing.common.actions.MarkSegmentsAsUnusedAction;
import org.apache.druid.indexing.common.actions.SegmentTransactionalAppendAction;
import org.apache.druid.indexing.common.actions.SegmentTransactionalInsertAction;
import org.apache.druid.indexing.common.actions.SegmentTransactionalReplaceAction;
import org.apache.druid.indexing.common.actions.TaskAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.task.batch.TooManyBucketsException;
import org.apache.druid.indexing.common.task.batch.parallel.TombstoneHelper;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.counters.CounterSnapshotsTree;
import org.apache.druid.msq.indexing.InputChannelFactory;
import org.apache.druid.msq.indexing.InputChannelsImpl;
import org.apache.druid.msq.indexing.MSQControllerTask;
import org.apache.druid.msq.indexing.MSQSpec;
import org.apache.druid.msq.indexing.MSQTuningConfig;
import org.apache.druid.msq.indexing.MSQWorkerTaskLauncher;
import org.apache.druid.msq.indexing.destination.DataSourceMSQDestination;
import org.apache.druid.msq.indexing.destination.DurableStorageMSQDestination;
import org.apache.druid.msq.indexing.destination.ExportMSQDestination;
import org.apache.druid.msq.indexing.destination.MSQSelectDestination;
import org.apache.druid.msq.indexing.destination.TaskReportMSQDestination;
import org.apache.druid.msq.indexing.error.CannotParseExternalDataFault;
import org.apache.druid.msq.indexing.error.FaultsExceededChecker;
import org.apache.druid.msq.indexing.error.InsertLockPreemptedFault;
import org.apache.druid.msq.indexing.error.MSQErrorReport;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.indexing.error.MSQFaultUtils;
import org.apache.druid.msq.indexing.error.MSQWarnings;
import org.apache.druid.msq.indexing.error.QueryNotSupportedFault;
import org.apache.druid.msq.indexing.error.TooManyBucketsFault;
import org.apache.druid.msq.indexing.error.UnknownFault;
import org.apache.druid.msq.indexing.processor.SegmentGeneratorFrameProcessorFactory;
import org.apache.druid.msq.indexing.report.MSQResultsReport;
import org.apache.druid.msq.indexing.report.MSQStagesReport;
import org.apache.druid.msq.indexing.report.MSQTaskReport;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.msq.input.InputSpecSlicerFactory;
import org.apache.druid.msq.input.InputSpecs;
import org.apache.druid.msq.input.stage.InputChannels;
import org.apache.druid.msq.input.stage.StageInputSpec;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.QueryDefinitionBuilder;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.kernel.StagePartition;
import org.apache.druid.msq.kernel.controller.ControllerQueryKernel;
import org.apache.druid.msq.kernel.controller.ControllerStagePhase;
import org.apache.druid.msq.querykit.QueryKit;
import org.apache.druid.msq.querykit.QueryKitUtils;
import org.apache.druid.msq.querykit.ShuffleSpecFactory;
import org.apache.druid.msq.querykit.results.ExportResultsFrameProcessorFactory;
import org.apache.druid.msq.querykit.results.QueryResultFrameProcessorFactory;
import org.apache.druid.msq.shuffle.input.DurableStorageInputChannelFactory;
import org.apache.druid.msq.shuffle.input.WorkerInputChannelFactory;
import org.apache.druid.msq.util.ArrayIngestMode;
import org.apache.druid.msq.util.DimensionSchemaUtils;
import org.apache.druid.msq.util.IntervalUtils;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.msq.util.PassthroughAggregatorFactory;
import org.apache.druid.msq.util.SqlStatementResourceHelper;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.ArbitraryGranularitySpec;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.sql.calcite.planner.ColumnMapping;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.storage.ExportStorageProvider;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.utils.CollectionUtils;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ControllerImpl extends AbstractController<MSQControllerTask>
{
  private static final Logger log = new Logger(ControllerImpl.class);

  /**
   * Queue of "commands" to run on the {@link ControllerQueryKernel}. Various threads insert into the queue
   * using {@link #addToKernelManipulationQueue}. The main thread running {@link RunQueryUntilDone#run()} reads
   * from the queue and executes the commands.
   * <p>
   * This ensures that all manipulations on {@link ControllerQueryKernel}, and all core logic, are run in
   * a single-threaded manner.
   */
  private final BlockingQueue<Consumer<ControllerQueryKernel>> kernelManipulationQueue =
      new ArrayBlockingQueue<>(Limits.MAX_KERNEL_MANIPULATION_QUEUE_SIZE);

  private volatile SegmentLoadStatusFetcher segmentLoadWaiter;

  public ControllerImpl(
      final MSQControllerTask task,
      final ControllerContext context
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

  private static TaskAction<SegmentPublishResult> createAppendAction(
      Set<DataSegment> segments,
      TaskLockType taskLockType
  )
  {
    if (taskLockType.equals(TaskLockType.APPEND)) {
      return SegmentTransactionalAppendAction.forSegments(segments);
    } else if (taskLockType.equals(TaskLockType.SHARED)) {
      return SegmentTransactionalInsertAction.appendAction(segments, null, null);
    } else {
      throw DruidException.defensive("Invalid lock type [%s] received for append action", taskLockType);
    }
  }


  private static DataSchema generateDataSchema(
      MSQSpec querySpec,
      RowSignature querySignature,
      ClusterBy queryClusterBy,
      ColumnMappings columnMappings,
      ObjectMapper jsonMapper
  )
  {
    final DataSourceMSQDestination destination = (DataSourceMSQDestination) querySpec.getDestination();
    final boolean isRollupQuery = isRollupQuery(querySpec.getQuery());

    final Pair<List<DimensionSchema>, List<AggregatorFactory>> dimensionsAndAggregators =
        makeDimensionsAndAggregatorsForIngestion(
            querySignature,
            queryClusterBy,
            destination.getSegmentSortOrder(),
            columnMappings,
            isRollupQuery,
            querySpec.getQuery()
        );

    return new DataSchema(
        destination.getDataSource(),
        new TimestampSpec(ColumnHolder.TIME_COLUMN_NAME, "millis", null),
        new DimensionsSpec(dimensionsAndAggregators.lhs),
        dimensionsAndAggregators.rhs.toArray(new AggregatorFactory[0]),
        makeGranularitySpecForIngestion(
            querySpec.getQuery(),
            querySpec.getColumnMappings(),
            isRollupQuery,
            jsonMapper
        ),
        new TransformSpec(null, Collections.emptyList())
    );
  }

  private static GranularitySpec makeGranularitySpecForIngestion(
      final Query<?> query,
      final ColumnMappings columnMappings,
      final boolean isRollupQuery,
      final ObjectMapper jsonMapper
  )
  {
    if (isRollupQuery) {
      final String queryGranularityString =
          query.context().getString(GroupByQuery.CTX_TIMESTAMP_RESULT_FIELD_GRANULARITY, "");

      if (timeIsGroupByDimension((GroupByQuery) query, columnMappings) && !queryGranularityString.isEmpty()) {
        final Granularity queryGranularity;

        try {
          queryGranularity = jsonMapper.readValue(queryGranularityString, Granularity.class);
        }
        catch (JsonProcessingException e) {
          throw new RuntimeException(e);
        }

        return new ArbitraryGranularitySpec(queryGranularity, true, Intervals.ONLY_ETERNITY);
      }
      return new ArbitraryGranularitySpec(Granularities.NONE, true, Intervals.ONLY_ETERNITY);
    } else {
      return new ArbitraryGranularitySpec(Granularities.NONE, false, Intervals.ONLY_ETERNITY);
    }
  }

  /**
   * Checks that a {@link GroupByQuery} is grouping on the primary time column.
   * <p>
   * The logic here is roundabout. First, we check which column in the {@link GroupByQuery} corresponds to the
   * output column {@link ColumnHolder#TIME_COLUMN_NAME}, using our {@link ColumnMappings}. Then, we check for the
   * presence of an optimization done in {@link DruidQuery#toGroupByQuery()}, where the context parameter
   * {@link GroupByQuery#CTX_TIMESTAMP_RESULT_FIELD} and various related parameters are set when one of the dimensions
   * is detected to be a time-floor. Finally, we check that the name of that dimension, and the name of our time field
   * from {@link ColumnMappings}, are the same.
   */
  private static boolean timeIsGroupByDimension(GroupByQuery groupByQuery, ColumnMappings columnMappings)
  {
    final IntList positions = columnMappings.getOutputColumnsByName(ColumnHolder.TIME_COLUMN_NAME);

    if (positions.size() == 1) {
      final String queryTimeColumn = columnMappings.getQueryColumnName(positions.getInt(0));
      return queryTimeColumn.equals(groupByQuery.context().getString(GroupByQuery.CTX_TIMESTAMP_RESULT_FIELD));
    } else {
      return false;
    }
  }

  /**
   * Whether a native query represents an ingestion with rollup.
   * <p>
   * Checks for three things:
   * <p>
   * - The query must be a {@link GroupByQuery}, because rollup requires columns to be split into dimensions and
   * aggregations.
   * - The query must not finalize aggregations, because rollup requires inserting the intermediate type of
   * complex aggregations, not the finalized type. (So further rollup is possible.)
   * - The query must explicitly disable {@link GroupByQueryConfig#CTX_KEY_ENABLE_MULTI_VALUE_UNNESTING}, because
   * groupBy on multi-value dimensions implicitly unnests, which is not desired behavior for rollup at ingestion time
   * (rollup expects multi-value dimensions to be treated as arrays).
   */
  private static boolean isRollupQuery(Query<?> query)
  {
    return query instanceof GroupByQuery
           && !MultiStageQueryContext.isFinalizeAggregations(query.context())
           && !query.context().getBoolean(GroupByQueryConfig.CTX_KEY_ENABLE_MULTI_VALUE_UNNESTING, true);
  }

  private static boolean isInlineResults(final MSQSpec querySpec)
  {
    return querySpec.getDestination() instanceof TaskReportMSQDestination
           || querySpec.getDestination() instanceof DurableStorageMSQDestination;
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

  private static MSQResultsReport makeResultsTaskReport(
      final QueryDefinition queryDef,
      final Yielder<Object[]> resultsYielder,
      final ColumnMappings columnMappings,
      @Nullable final List<SqlTypeName> sqlTypeNames,
      final MSQSelectDestination selectDestination
  )
  {
    final RowSignature querySignature = queryDef.getFinalStageDefinition().getSignature();
    final ImmutableList.Builder<MSQResultsReport.ColumnAndType> mappedSignature = ImmutableList.builder();

    for (final ColumnMapping mapping : columnMappings.getMappings()) {
      mappedSignature.add(
          new MSQResultsReport.ColumnAndType(
              mapping.getOutputColumn(),
              querySignature.getColumnType(mapping.getQueryColumn()).orElse(null)
          )
      );
    }

    return MSQResultsReport.createReportAndLimitRowsIfNeeded(
        mappedSignature.build(),
        sqlTypeNames,
        resultsYielder,
        selectDestination
    );
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

  static ClusterStatisticsMergeMode finalizeClusterStatisticsMergeMode(
      StageDefinition stageDef,
      ClusterStatisticsMergeMode initialMode
  )
  {
    ClusterStatisticsMergeMode mergeMode = initialMode;
    if (initialMode == ClusterStatisticsMergeMode.AUTO) {
      ClusterBy clusterBy = stageDef.getClusterBy();
      if (clusterBy.getBucketByCount() == 0) {
        // If there is no time clustering, there is no scope for sequential merge
        mergeMode = ClusterStatisticsMergeMode.PARALLEL;
      } else if (stageDef.getMaxWorkerCount() > Limits.MAX_WORKERS_FOR_PARALLEL_MERGE) {
        mergeMode = ClusterStatisticsMergeMode.SEQUENTIAL;
      } else {
        mergeMode = ClusterStatisticsMergeMode.PARALLEL;
      }
      log.info(
          "Stage [%d] AUTO mode: chose %s mode to merge key statistics",
          stageDef.getStageNumber(),
          mergeMode
      );
    }
    return mergeMode;
  }

  @SuppressWarnings("unchecked")
  private static QueryDefinition makeQueryDefinition(
      final String queryId,
      @SuppressWarnings("rawtypes") final QueryKit toolKit,
      final MSQSpec querySpec,
      final ObjectMapper jsonMapper
  )
  {
    final MSQTuningConfig tuningConfig = querySpec.getTuningConfig();
    final ColumnMappings columnMappings = querySpec.getColumnMappings();
    final Query<?> queryToPlan;
    final ShuffleSpecFactory shuffleSpecFactory;

    if (MSQControllerTask.isIngestion(querySpec)) {
      shuffleSpecFactory = querySpec.getDestination()
                                    .getShuffleSpecFactory(tuningConfig.getRowsPerSegment());

      if (!columnMappings.hasUniqueOutputColumnNames()) {
        // We do not expect to hit this case in production, because the SQL validator checks that column names
        // are unique for INSERT and REPLACE statements (i.e. anything where MSQControllerTask.isIngestion would
        // be true). This check is here as defensive programming.
        throw new ISE("Column names are not unique: [%s]", columnMappings.getOutputColumnNames());
      }

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
      shuffleSpecFactory = querySpec.getDestination()
                                    .getShuffleSpecFactory(MultiStageQueryContext.getRowsPerPage(querySpec.getQuery()
                                                                                                          .context()));
      queryToPlan = querySpec.getQuery();
    }

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

    if (MSQControllerTask.isIngestion(querySpec)) {
      final RowSignature querySignature = queryDef.getFinalStageDefinition().getSignature();
      final ClusterBy queryClusterBy = queryDef.getFinalStageDefinition().getClusterBy();

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
      final QueryDefinitionBuilder builder = QueryDefinition.builder();

      for (final StageDefinition stageDef : queryDef.getStageDefinitions()) {
        if (stageDef.equals(finalShuffleStageDef)) {
          builder.add(StageDefinition.builder(stageDef).shuffleCheckHasMultipleValues(true));
        } else {
          builder.add(StageDefinition.builder(stageDef));
        }
      }

      // Then, add a segment-generation stage.
      final DataSchema dataSchema =
          generateDataSchema(querySpec, querySignature, queryClusterBy, columnMappings, jsonMapper);

      builder.add(
          StageDefinition.builder(queryDef.getNextStageNumber())
                         .inputs(new StageInputSpec(queryDef.getFinalStageDefinition().getStageNumber()))
                         .maxWorkerCount(tuningConfig.getMaxNumWorkers())
                         .processorFactory(
                             new SegmentGeneratorFrameProcessorFactory(
                                 dataSchema,
                                 columnMappings,
                                 tuningConfig
                             )
                         )
      );

      return builder.build();
    } else if (querySpec.getDestination() instanceof TaskReportMSQDestination) {
      return queryDef;
    } else if (querySpec.getDestination() instanceof DurableStorageMSQDestination) {

      // attaching new query results stage if the final stage does sort during shuffle so that results are ordered.
      StageDefinition finalShuffleStageDef = queryDef.getFinalStageDefinition();
      if (finalShuffleStageDef.doesSortDuringShuffle()) {
        final QueryDefinitionBuilder builder = QueryDefinition.builder();
        builder.addAll(queryDef);
        builder.add(StageDefinition.builder(queryDef.getNextStageNumber())
                                   .inputs(new StageInputSpec(queryDef.getFinalStageDefinition().getStageNumber()))
                                   .maxWorkerCount(tuningConfig.getMaxNumWorkers())
                                   .signature(finalShuffleStageDef.getSignature())
                                   .shuffleSpec(null)
                                   .processorFactory(new QueryResultFrameProcessorFactory())
        );
        return builder.build();
      } else {
        return queryDef;
      }
    } else if (querySpec.getDestination() instanceof ExportMSQDestination) {
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
                                     columnMappings
                                 ))
      );
      return builder.build();
    } else {
      throw new ISE("Unsupported destination [%s]", querySpec.getDestination());
    }
  }

  @Override
  public String id()
  {
    return task.getId();
  }

  @Override
  public MSQControllerTask task()
  {
    return task;
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
      resultsYielder = getFinalResultsYielder(queryDef, queryKernel);
      publishSegmentsIfNeeded(queryDef, queryKernel);
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
        resultsReport = makeResultsTaskReport(
            queryDef,
            resultsYielder,
            task.getQuerySpec().getColumnMappings(),
            task.getSqlTypeNames(),
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

    try {
      releaseTaskLocks();
      cleanUpDurableStorageIfNeeded();

      if (queryKernel != null && queryKernel.isSuccess()) {
        if (shouldWaitForSegmentLoad && segmentLoadWaiter != null) {
          // If successful, there are segments created and segment load is enabled, segmentLoadWaiter should wait
          // for them to become available.
          log.info("Controller will now wait for segments to be loaded. The query has already finished executing,"
                   + " and results will be included once the segments are loaded, even if this query is cancelled now.");
          segmentLoadWaiter.waitForSegmentsToLoad();
        }
      }
      stopExternalFetchers();
    }
    catch (Exception e) {
      log.warn(e, "Exception thrown during cleanup. Ignoring it and writing task report.");
    }

    try {
      // Write report even if something went wrong.
      final MSQStagesReport stagesReport;

      if (queryDef != null) {
        final Map<Integer, ControllerStagePhase> stagePhaseMap;

        if (queryKernel != null) {
          // Once the query finishes, cleanup would have happened for all the stages that were successful
          // Therefore we mark it as done to make the reports prettier and more accurate
          queryKernel.markSuccessfulTerminalStagesAsFinished();
          stagePhaseMap = queryKernel.getActiveStages()
                                     .stream()
                                     .collect(
                                         Collectors.toMap(StageId::getStageNumber, queryKernel::getStagePhase)
                                     );
        } else {
          stagePhaseMap = Collections.emptyMap();
        }

        stagesReport = makeStageReport(
            queryDef,
            stagePhaseMap,
            stageRuntimesForLiveReports,
            stageWorkerCountsForLiveReports,
            stagePartitionCountsForLiveReports
        );
      } else {
        stagesReport = null;
      }

      final MSQTaskReportPayload taskReportPayload = new MSQTaskReportPayload(
          makeStatusReport(
              taskStateForReport,
              errorForReport,
              workerWarnings,
              queryStartTime,
              new Interval(queryStartTime, DateTimes.nowUtc()).toDurationMillis(),
              workerTaskLauncher,
              segmentLoadWaiter
          ),
          stagesReport,
          countersSnapshot,
          resultsReport
      );
      context.writeReports(
          id(),
          TaskReport.buildTaskReports(new MSQTaskReport(id(), taskReportPayload))
      );
    }
    catch (Throwable e) {
      log.warn(e, "Error encountered while writing task report. Skipping.");
    }

    if (taskStateForReport == TaskState.SUCCESS) {
      return TaskStatus.success(id());
    } else {
      // errorForReport is nonnull when taskStateForReport != SUCCESS. Use that message.
      return TaskStatus.failure(id(), MSQFaultUtils.generateMessageWithErrorCode(errorForReport.getFault()));
    }
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
        task.getQuerySpec(),
        context.jsonMapper()
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

    if (!MSQControllerTask.isIngestion(task.getQuerySpec())) {
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
    }

    taskContextOverridesBuilder.put(
        MultiStageQueryContext.CTX_IS_REINDEX,
        MSQControllerTask.isReplaceInputDataSourceTask(task)
    );

    this.workerTaskLauncher = new MSQWorkerTaskLauncher(
        id(),
        task.getDataSource(),
        context,
        (failedTask, fault) -> {
          if (isFaultToleranceEnabled && ControllerQueryKernel.isRetriableFault(fault)) {
            addToKernelManipulationQueue((kernel) -> {
              addToRetryQueue(kernel, failedTask.getWorkerNumber(), fault);
            });
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
                finalizeClusterStatisticsMergeMode(
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

  /**
   * Publish the list of segments. Additionally, if {@link DataSourceMSQDestination#isReplaceTimeChunks()},
   * also drop all other segments within the replacement intervals.
   */
  private void publishAllSegments(final Set<DataSegment> segments) throws IOException
  {
    final DataSourceMSQDestination destination =
        (DataSourceMSQDestination) task.getQuerySpec().getDestination();
    final Set<DataSegment> segmentsWithTombstones = new HashSet<>(segments);
    int numTombstones = 0;
    final TaskLockType taskLockType = MultiStageQueryContext.validateAndGetTaskLockType(
        QueryContext.of(task.getQuerySpec().getQuery().getContext()),
        destination.isReplaceTimeChunks()
    );

    if (destination.isReplaceTimeChunks()) {
      final List<Interval> intervalsToDrop = findIntervalsToDrop(Preconditions.checkNotNull(segments, "segments"));

      if (!intervalsToDrop.isEmpty()) {
        TombstoneHelper tombstoneHelper = new TombstoneHelper(context.taskActionClient());
        try {
          Set<DataSegment> tombstones = tombstoneHelper.computeTombstoneSegmentsForReplace(
              intervalsToDrop,
              destination.getReplaceTimeChunks(),
              task.getDataSource(),
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
                 .submit(new MarkSegmentsAsUnusedAction(task.getDataSource(), interval));
        }
      } else {
        if (MultiStageQueryContext.shouldWaitForSegmentLoad(task.getQuerySpec().getQuery().context())) {
          segmentLoadWaiter = new SegmentLoadStatusFetcher(
              context.injector().getInstance(BrokerClient.class),
              context.jsonMapper(),
              task.getId(),
              task.getDataSource(),
              segmentsWithTombstones,
              true
          );
        }
        performSegmentPublish(
            context.taskActionClient(),
            createOverwriteAction(taskLockType, segmentsWithTombstones)
        );
      }
    } else if (!segments.isEmpty()) {
      if (MultiStageQueryContext.shouldWaitForSegmentLoad(task.getQuerySpec().getQuery().context())) {
        segmentLoadWaiter = new SegmentLoadStatusFetcher(
            context.injector().getInstance(BrokerClient.class),
            context.jsonMapper(),
            task.getId(),
            task.getDataSource(),
            segments,
            true
        );
      }
      // Append mode.
      performSegmentPublish(
          context.taskActionClient(),
          createAppendAction(segments, taskLockType)
      );
    }

    task.emitMetric(context.emitter(), "ingest/tombstones/count", numTombstones);
    // Include tombstones in the reported segments count
    task.emitMetric(context.emitter(), "ingest/segments/count", segmentsWithTombstones.size());
  }

  private TaskAction<SegmentPublishResult> createOverwriteAction(
      TaskLockType taskLockType,
      Set<DataSegment> segmentsWithTombstones
  )
  {
    if (taskLockType.equals(TaskLockType.REPLACE)) {
      return SegmentTransactionalReplaceAction.create(segmentsWithTombstones);
    } else if (taskLockType.equals(TaskLockType.EXCLUSIVE)) {
      return SegmentTransactionalInsertAction.overwriteAction(null, segmentsWithTombstones);
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
        (DataSourceMSQDestination) task.getQuerySpec().getDestination();
    final List<Interval> replaceIntervals =
        new ArrayList<>(JodaUtils.condenseIntervals(destination.getReplaceTimeChunks()));
    final List<Interval> publishIntervals =
        JodaUtils.condenseIntervals(Iterables.transform(publishedSegments, DataSegment::getInterval));
    return IntervalUtils.difference(replaceIntervals, publishIntervals);
  }

  @Nullable
  private Yielder<Object[]> getFinalResultsYielder(
      final QueryDefinition queryDef,
      final ControllerQueryKernel queryKernel
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
          Sequences.concat(
                       StreamSupport.stream(queryKernel.getResultPartitionsForStage(finalStageId).spliterator(), false)
                                    .map(
                                        readablePartition -> {
                                          try {
                                            return new FrameChannelSequence(
                                                inputChannels.openChannel(
                                                    new StagePartition(
                                                        queryKernel.getStageDefinition(finalStageId).getId(),
                                                        readablePartition.getPartitionNumber()
                                                    )
                                                )
                                            );
                                          }
                                          catch (IOException e) {
                                            throw new RuntimeException(e);
                                          }
                                        }
                                    ).collect(Collectors.toList())
                   ).flatMap(
                       frame ->
                           SqlStatementResourceHelper.getResultSequence(
                               task,
                               queryDef.getFinalStageDefinition(),
                               frame,
                               context.jsonMapper()
                           )
                   )
                   .withBaggage(resultReaderExec::shutdownNow)
      );
    } else {
      return null;
    }
  }

  private void publishSegmentsIfNeeded(
      final QueryDefinition queryDef,
      final ControllerQueryKernel queryKernel
  ) throws IOException
  {
    if (queryKernel.isSuccess() && MSQControllerTask.isIngestion(task.getQuerySpec())) {
      final StageId finalStageId = queryKernel.getStageId(queryDef.getFinalStageDefinition().getStageNumber());

      //noinspection unchecked
      @SuppressWarnings("unchecked")
      final Set<DataSegment> segments = (Set<DataSegment>) queryKernel.getResultObjectForStage(finalStageId);
      log.info("Query [%s] publishing %d segments.", queryDef.getQueryId(), segments.size());
      publishAllSegments(segments);
    }
  }
}
