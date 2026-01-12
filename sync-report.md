# MSQ Sync Report

## [IMPORTANT] Remember to update pom.xml and README.md manually

## Manual port required (owned & changed)
- src/main/java/org/apache/druid/msq/dart/controller/sql/DartQueryMaker.java +/-
- src/main/java/org/apache/druid/msq/exec/Controller.java
- src/main/java/org/apache/druid/msq/exec/ControllerImpl.java
- src/main/java/org/apache/druid/msq/guice/MSQIndexingModule.java +
- src/main/java/org/apache/druid/msq/indexing/IndexerControllerContext.java +
- src/main/java/org/apache/druid/msq/indexing/MSQControllerTask.java +
- src/main/java/org/apache/druid/msq/sql/MSQTaskQueryMaker.java
- src/main/java/org/apache/druid/msq/sql/resources/SqlStatementResource.java
- src/test/java/org/apache/druid/msq/exec/ControllerImplTest.java
- src/test/java/org/apache/druid/msq/exec/TestMSQSqlModule.java
- src/test/java/org/apache/druid/msq/sql/resources/SqlStatementResourceTest.java
- src/test/java/org/apache/druid/msq/test/MSQTestBase.java
- src/test/java/org/apache/druid/msq/test/MSQTestOverlordServiceClient.java
- src/test/java/org/apache/druid/sql/avatica/MSQDruidMeta.java

### Method-level diffs
### src/main/java/org/apache/druid/msq/dart/controller/sql/DartQueryMaker.java
- Added methods:
  - `org.apache.druid.msq.dart.controller.sql.DartQueryMaker#DartQueryMaker(List, DartControllerContextFactory, PlannerContext, DartControllerRegistry, DartControllerConfig, ExecutorService, QueryKitSpecFactory, ServerConfig)`
  - `org.apache.druid.msq.dart.controller.sql.DartQueryMaker#makeResultsContext(DruidQuery, List, PlannerContext)`
  - `org.apache.druid.msq.dart.controller.sql.DartQueryMaker#runLegacyMSQSpec(LegacyMSQSpec, QueryContext, ResultsContext)`
  - `org.apache.druid.msq.dart.controller.sql.DartQueryMaker#runQueryDefMSQSpec(QueryDefMSQSpec, QueryContext, ResultsContext)`
  - `org.apache.druid.msq.dart.controller.sql.DartQueryMaker.ResultIterator#ResultIterator(Duration)`
- Removed methods:
  - `org.apache.druid.msq.dart.controller.sql.DartQueryMaker#DartQueryMaker(List, DartControllerContextFactory, PlannerContext, DartControllerRegistry, DartControllerConfig, ExecutorService)`

### src/main/java/org/apache/druid/msq/exec/Controller.java
- Added methods:
  - `org.apache.druid.msq.exec.Controller#getControllerContext()`
  - `org.apache.druid.msq.exec.Controller#getQueryContext()`
  - `org.apache.druid.msq.exec.Controller#stop(CancellationReason)`
- Removed methods:
  - `org.apache.druid.msq.exec.Controller#stop()`

### src/main/java/org/apache/druid/msq/exec/ControllerImpl.java
- Added methods:
  - `org.apache.druid.msq.exec.ControllerImpl#ControllerImpl(LegacyMSQSpec, ResultsContext, ControllerContext, QueryKitSpecFactory)`
  - `org.apache.druid.msq.exec.ControllerImpl#ControllerImpl(QueryDefMSQSpec, ResultsContext, ControllerContext, QueryKitSpecFactory)`
  - `org.apache.druid.msq.exec.ControllerImpl#buildMSQCompactionMetrics(MSQSpec, DataSchema)`
  - `org.apache.druid.msq.exec.ControllerImpl#ensureExportLocationEmpty(ControllerContext, MSQDestination)`
  - `org.apache.druid.msq.exec.ControllerImpl#getControllerContext()`
  - `org.apache.druid.msq.exec.ControllerImpl#getQueryContext()`
  - `org.apache.druid.msq.exec.ControllerImpl#stop(CancellationReason)`
- Removed methods:
  - `org.apache.druid.msq.exec.ControllerImpl#ControllerImpl(String, MSQSpec, ResultsContext, ControllerContext)`
  - `org.apache.druid.msq.exec.ControllerImpl#stop()`

### src/main/java/org/apache/druid/msq/guice/MSQIndexingModule.java
- Signature changes: none

### src/main/java/org/apache/druid/msq/indexing/IndexerControllerContext.java
- Added methods:
  - `org.apache.druid.msq.indexing.IndexerControllerContext#IndexerControllerContext(MSQControllerTask, TaskToolbox, Injector, ServiceClientFactory, OverlordClient)`
  - `org.apache.druid.msq.indexing.IndexerControllerContext#emitMetric(MSQMetriceEventBuilder)`
  - `org.apache.druid.msq.indexing.IndexerControllerContext#newWorkerManager(String, MSQSpec, ControllerQueryKernelConfig)`
  - `org.apache.druid.msq.indexing.IndexerControllerContext#queryId()`
  - `org.apache.druid.msq.indexing.IndexerControllerContext#queryKernelConfig(MSQSpec)`
- Removed methods:
  - `org.apache.druid.msq.indexing.IndexerControllerContext#IndexerControllerContext(TaskLockType, String, QueryContext, Map, ServiceMetricEvent.Builder, TaskToolbox, Injector, ServiceClientFactory, OverlordClient)`
  - `org.apache.druid.msq.indexing.IndexerControllerContext#emitMetric(String, Number)`
  - `org.apache.druid.msq.indexing.IndexerControllerContext#makeQueryKitSpec(QueryKit, String, MSQSpec, ControllerQueryKernelConfig)`
  - `org.apache.druid.msq.indexing.IndexerControllerContext#newWorkerManager(String, MSQSpec, ControllerQueryKernelConfig, WorkerFailureListener)`
  - `org.apache.druid.msq.indexing.IndexerControllerContext#queryKernelConfig(String, MSQSpec)`

### src/main/java/org/apache/druid/msq/indexing/MSQControllerTask.java
- Added methods:
  - `org.apache.druid.msq.indexing.MSQControllerTask#MSQControllerTask(String, LegacyMSQSpec, String, Map, SqlResults.Context, List, List, Map)`
  - `org.apache.druid.msq.indexing.MSQControllerTask#MSQControllerTask(String, LegacyMSQSpec, String, Map, SqlResults.Context, List, List, Map, Injector)`
- Removed methods:
  - `org.apache.druid.msq.indexing.MSQControllerTask#MSQControllerTask(String, MSQSpec, String, Map, SqlResults.Context, List, List, Map)`
  - `org.apache.druid.msq.indexing.MSQControllerTask#MSQControllerTask(String, MSQSpec, String, Map, SqlResults.Context, List, List, Map, Injector)`

### src/main/java/org/apache/druid/msq/sql/MSQTaskQueryMaker.java
- Added methods:
  - `org.apache.druid.msq.sql.MSQTaskQueryMaker#makeLegacyMSQSpec(IngestDestination, DruidQuery, QueryContext, ColumnMappings, PlannerContext, MSQTerminalStageSpecFactory)`
  - `org.apache.druid.msq.sql.MSQTaskQueryMaker#makeQueryDefMSQSpec(IngestDestination, QueryContext, ColumnMappings, PlannerContext, MSQTerminalStageSpecFactory, QueryDefinition)`
  - `org.apache.druid.msq.sql.MSQTaskQueryMaker#makeResultsContext(DruidQuery, List, PlannerContext)`
  - `org.apache.druid.msq.sql.MSQTaskQueryMaker#makeSimpleResultContext(QueryDefinition, RelDataType, List, PlannerContext)`
- Removed methods:
  - `org.apache.druid.msq.sql.MSQTaskQueryMaker#makeQuerySpec(IngestDestination, DruidQuery, List, PlannerContext, MSQTerminalStageSpecFactory)`

### src/main/java/org/apache/druid/msq/sql/resources/SqlStatementResource.java
- Added methods:
  - `org.apache.druid.msq.sql.resources.SqlStatementResource#doPost(HttpServletRequest, HttpContext)`
- Removed methods:
  - `org.apache.druid.msq.sql.resources.SqlStatementResource#doPost(SqlQuery, HttpServletRequest)`

### src/test/java/org/apache/druid/msq/exec/ControllerImplTest.java
- Signature changes: none

### src/test/java/org/apache/druid/msq/exec/TestMSQSqlModule.java
- Added methods:
  - `org.apache.druid.msq.exec.TestMSQSqlModule#createEngine(ObjectMapper, MSQTestOverlordServiceClient, MSQTaskQueryKitSpecFactory)`
- Removed methods:
  - `org.apache.druid.msq.exec.TestMSQSqlModule#createEngine(ObjectMapper, MSQTestOverlordServiceClient)`

### src/test/java/org/apache/druid/msq/sql/resources/SqlStatementResourceTest.java
- Signature changes: none

### src/test/java/org/apache/druid/msq/test/MSQTestBase.java
- Added methods:
  - `org.apache.druid.msq.test.MSQTestBase#getEmittedMetrics(String, Map)`
  - `org.apache.druid.msq.test.MSQTestBase.MSQTester#setExpectedMSQSpec(LegacyMSQSpec)`
  - `org.apache.druid.msq.test.MSQTestBase.MSQTester#setExpectedMetricDimensions(Map)`
  - `org.apache.druid.msq.test.MSQTestBase.MSQTester#verifyMetrics()`
- Removed methods:
  - `org.apache.druid.msq.test.MSQTestBase.MSQTester#setExpectedMSQSpec(MSQSpec)`

### src/test/java/org/apache/druid/msq/test/MSQTestOverlordServiceClient.java
- Added methods:
  - `org.apache.druid.msq.test.MSQTestOverlordServiceClient#closeTask(String)`
  - `org.apache.druid.msq.test.MSQTestOverlordServiceClient#getEmittedMetrics(String, Map)`
  - `org.apache.druid.msq.test.MSQTestOverlordServiceClient.MSQTestTaskDetails#addController(ControllerImpl)`
  - `org.apache.druid.msq.test.MSQTestOverlordServiceClient.MSQTestTaskDetails#close()`
  - `org.apache.druid.msq.test.MSQTestOverlordServiceClient.MSQTestTaskDetails#getController(String)`

### src/test/java/org/apache/druid/sql/avatica/MSQDruidMeta.java
- Signature changes: none

## Auto-applied (changed but not owned)
- src/main/java/org/apache/druid/msq/counters/CounterTracker.java
- src/main/java/org/apache/druid/msq/dart/DartResourcePermissionMapper.java
- src/main/java/org/apache/druid/msq/dart/controller/ControllerHolder.java
- src/main/java/org/apache/druid/msq/dart/controller/DartControllerContext.java
- src/main/java/org/apache/druid/msq/dart/controller/DartControllerContextFactory.java
- src/main/java/org/apache/druid/msq/dart/controller/DartControllerContextFactoryImpl.java
- src/main/java/org/apache/druid/msq/dart/controller/DartTableInputSpecSlicer.java
- src/main/java/org/apache/druid/msq/dart/controller/DartWorkerManager.java
- src/main/java/org/apache/druid/msq/dart/controller/http/DartQueryInfo.java
- src/main/java/org/apache/druid/msq/dart/controller/sql/DartSqlClient.java
- src/main/java/org/apache/druid/msq/dart/controller/sql/DartSqlClientFactoryImpl.java
- src/main/java/org/apache/druid/msq/dart/controller/sql/DartSqlClientImpl.java
- src/main/java/org/apache/druid/msq/dart/controller/sql/DartSqlClients.java
- src/main/java/org/apache/druid/msq/dart/controller/sql/DartSqlEngine.java
- src/main/java/org/apache/druid/msq/dart/guice/DartControllerConfig.java
- src/main/java/org/apache/druid/msq/dart/guice/DartControllerMemoryManagementModule.java
- src/main/java/org/apache/druid/msq/dart/guice/DartControllerModule.java
- src/main/java/org/apache/druid/msq/dart/guice/DartWorkerMemoryManagementModule.java
- src/main/java/org/apache/druid/msq/dart/guice/DartWorkerModule.java
- src/main/java/org/apache/druid/msq/dart/worker/DartDataSegmentProvider.java
- src/main/java/org/apache/druid/msq/dart/worker/DartFrameContext.java
- src/main/java/org/apache/druid/msq/dart/worker/DartProcessingBuffersProvider.java
- src/main/java/org/apache/druid/msq/dart/worker/DartQueryableSegment.java
- src/main/java/org/apache/druid/msq/dart/worker/DartWorkerClientImpl.java
- src/main/java/org/apache/druid/msq/dart/worker/DartWorkerContext.java
- src/main/java/org/apache/druid/msq/dart/worker/DartWorkerRunner.java
- src/main/java/org/apache/druid/msq/exec/ControllerContext.java
- src/main/java/org/apache/druid/msq/exec/DataServerQueryHandler.java
- src/main/java/org/apache/druid/msq/exec/DataServerQueryHandlerFactory.java
- src/main/java/org/apache/druid/msq/exec/ListeningOutputChannelFactory.java
- src/main/java/org/apache/druid/msq/exec/ProcessingBuffers.java
- src/main/java/org/apache/druid/msq/exec/ProcessingBuffersProvider.java
- src/main/java/org/apache/druid/msq/exec/ProcessingBuffersSet.java
- src/main/java/org/apache/druid/msq/exec/QueryKitBasedMSQPlanner.java
- src/main/java/org/apache/druid/msq/exec/ResultsContext.java
- src/main/java/org/apache/druid/msq/exec/RetryCapableWorkerManager.java
- src/main/java/org/apache/druid/msq/exec/RunWorkOrder.java
- src/main/java/org/apache/druid/msq/exec/SegmentLoadStatusFetcher.java
- src/main/java/org/apache/druid/msq/exec/SegmentSource.java
- src/main/java/org/apache/druid/msq/exec/Worker.java
- src/main/java/org/apache/druid/msq/exec/WorkerContext.java
- src/main/java/org/apache/druid/msq/exec/WorkerImpl.java
- src/main/java/org/apache/druid/msq/exec/WorkerManager.java
- src/main/java/org/apache/druid/msq/exec/WorkerMemoryParameters.java
- src/main/java/org/apache/druid/msq/exec/WorkerSketchFetcher.java
- src/main/java/org/apache/druid/msq/guice/IndexerMemoryManagementModule.java
- src/main/java/org/apache/druid/msq/guice/PeonMemoryManagementModule.java
- src/main/java/org/apache/druid/msq/indexing/CountingOutputChannelFactory.java
- src/main/java/org/apache/druid/msq/indexing/IndexerFrameContext.java
- src/main/java/org/apache/druid/msq/indexing/IndexerProcessingBuffersProvider.java
- src/main/java/org/apache/druid/msq/indexing/IndexerWorkerContext.java
- src/main/java/org/apache/druid/msq/indexing/InputChannelsImpl.java
- src/main/java/org/apache/druid/msq/indexing/MSQCompactionRunner.java
- src/main/java/org/apache/druid/msq/indexing/MSQSpec.java
- src/main/java/org/apache/druid/msq/indexing/MSQWorkerTask.java
- src/main/java/org/apache/druid/msq/indexing/MSQWorkerTaskLauncher.java
- src/main/java/org/apache/druid/msq/indexing/PeonProcessingBuffersProvider.java
- src/main/java/org/apache/druid/msq/indexing/destination/DataSourceMSQDestination.java
- src/main/java/org/apache/druid/msq/indexing/destination/MSQTerminalStageSpecFactory.java
- src/main/java/org/apache/druid/msq/indexing/destination/SegmentGenerationStageSpec.java
- src/main/java/org/apache/druid/msq/indexing/destination/SegmentGenerationTerminalStageSpecFactory.java
- src/main/java/org/apache/druid/msq/indexing/destination/SegmentGenerationUtils.java
- src/main/java/org/apache/druid/msq/indexing/destination/TerminalStageSpec.java
- src/main/java/org/apache/druid/msq/indexing/error/CanceledFault.java
- src/main/java/org/apache/druid/msq/indexing/error/MSQErrorReport.java
- src/main/java/org/apache/druid/msq/indexing/processor/KeyStatisticsCollectionProcessor.java
- src/main/java/org/apache/druid/msq/input/ParseExceptionUtils.java
- src/main/java/org/apache/druid/msq/input/external/ExternalSegment.java
- src/main/java/org/apache/druid/msq/input/table/DataSegmentWithLocation.java
- src/main/java/org/apache/druid/msq/input/table/RichSegmentDescriptor.java
- src/main/java/org/apache/druid/msq/input/table/SegmentsInputSliceReader.java
- src/main/java/org/apache/druid/msq/kernel/NilExtraInfoHolder.java
- src/main/java/org/apache/druid/msq/kernel/QueryDefinition.java
- src/main/java/org/apache/druid/msq/kernel/QueryDefinitionBuilder.java
- src/main/java/org/apache/druid/msq/kernel/StageDefinition.java
- src/main/java/org/apache/druid/msq/kernel/StageDefinitionBuilder.java
- src/main/java/org/apache/druid/msq/kernel/WorkOrder.java
- src/main/java/org/apache/druid/msq/kernel/controller/ControllerQueryKernel.java
- src/main/java/org/apache/druid/msq/kernel/controller/ControllerQueryKernelConfig.java
- src/main/java/org/apache/druid/msq/kernel/controller/ControllerStageTracker.java
- src/main/java/org/apache/druid/msq/querykit/BaseLeafFrameProcessor.java
- src/main/java/org/apache/druid/msq/querykit/BaseLeafFrameProcessorManager.java
- src/main/java/org/apache/druid/msq/querykit/BroadcastJoinSegmentMapFnProcessor.java
- src/main/java/org/apache/druid/msq/querykit/DataSourcePlan.java
- src/main/java/org/apache/druid/msq/querykit/InputNumberDataSource.java
- src/main/java/org/apache/druid/msq/querykit/RestrictedInputNumberDataSource.java
- src/main/java/org/apache/druid/msq/querykit/SimpleSegmentMapFnProcessor.java
- src/main/java/org/apache/druid/msq/querykit/WindowOperatorQueryKit.java
- src/main/java/org/apache/druid/msq/querykit/common/SortMergeJoinFrameProcessor.java
- src/main/java/org/apache/druid/msq/querykit/groupby/GroupByPostShuffleFrameProcessor.java
- src/main/java/org/apache/druid/msq/querykit/groupby/GroupByPreShuffleFrameProcessor.java
- src/main/java/org/apache/druid/msq/querykit/groupby/GroupByQueryKit.java
- src/main/java/org/apache/druid/msq/querykit/results/ExportResultsFrameProcessor.java
- src/main/java/org/apache/druid/msq/querykit/scan/ScanQueryFrameProcessor.java
- src/main/java/org/apache/druid/msq/querykit/scan/ScanQueryKit.java
- src/main/java/org/apache/druid/msq/shuffle/output/DurableStorageOutputChannelFactory.java
- src/main/java/org/apache/druid/msq/sql/MSQTaskSqlEngine.java
- src/main/java/org/apache/druid/msq/sql/resources/SqlTaskResource.java
- src/main/java/org/apache/druid/msq/statistics/ClusterByStatisticsCollectorImpl.java
- src/main/java/org/apache/druid/msq/util/MSQTaskQueryMakerUtils.java
- src/main/java/org/apache/druid/msq/util/MultiStageQueryContext.java
- src/test/java/org/apache/druid/msq/dart/controller/DartControllerContextTest.java
- src/test/java/org/apache/druid/msq/dart/controller/DartTableInputSpecSlicerTest.java
- src/test/java/org/apache/druid/msq/dart/controller/DartWorkerManagerTest.java
- src/test/java/org/apache/druid/msq/dart/controller/http/DartQueryInfoTest.java
- src/test/java/org/apache/druid/msq/dart/controller/http/DartSqlResourceTest.java
- src/test/java/org/apache/druid/msq/dart/controller/sql/DartSqlClientImplTest.java
- src/test/java/org/apache/druid/msq/dart/worker/DartWorkerRunnerTest.java
- src/test/java/org/apache/druid/msq/exec/MSQArraysTest.java
- src/test/java/org/apache/druid/msq/exec/MSQComplexGroupByTest.java
- src/test/java/org/apache/druid/msq/exec/MSQDataSketchesTest.java
- src/test/java/org/apache/druid/msq/exec/MSQInsertTest.java
- src/test/java/org/apache/druid/msq/exec/MSQLoadedSegmentTests.java
- src/test/java/org/apache/druid/msq/exec/MSQParseExceptionsTest.java
- src/test/java/org/apache/druid/msq/exec/MSQReplaceTest.java
- src/test/java/org/apache/druid/msq/exec/MSQSelectTest.java
- src/test/java/org/apache/druid/msq/exec/MSQTasksTest.java
- src/test/java/org/apache/druid/msq/exec/MSQWindowTest.java
- src/test/java/org/apache/druid/msq/exec/QueryValidatorTest.java
- src/test/java/org/apache/druid/msq/exec/ResultsContextSerdeTest.java
- src/test/java/org/apache/druid/msq/exec/RunWorkOrderTest.java
- src/test/java/org/apache/druid/msq/exec/SegmentLoadStatusFetcherTest.java
- src/test/java/org/apache/druid/msq/indexing/MSQCompactionRunnerTest.java
- src/test/java/org/apache/druid/msq/indexing/MSQControllerTaskTest.java
- src/test/java/org/apache/druid/msq/indexing/MSQSpecTest.java
- src/test/java/org/apache/druid/msq/indexing/MSQWorkerTaskLauncherTest.java
- src/test/java/org/apache/druid/msq/indexing/WorkerChatHandlerTest.java
- src/test/java/org/apache/druid/msq/indexing/error/InsertLockPreemptedFaultTest.java
- src/test/java/org/apache/druid/msq/indexing/error/MSQFaultSerdeTest.java
- src/test/java/org/apache/druid/msq/indexing/error/MSQWarningsTest.java
- src/test/java/org/apache/druid/msq/indexing/report/MSQTaskReportTest.java
- src/test/java/org/apache/druid/msq/kernel/QueryDefinitionTest.java
- src/test/java/org/apache/druid/msq/kernel/StageDefinitionTest.java
- src/test/java/org/apache/druid/msq/kernel/controller/BaseControllerQueryKernelTest.java
- src/test/java/org/apache/druid/msq/kernel/controller/MockQueryDefinitionBuilder.java
- src/test/java/org/apache/druid/msq/kernel/controller/WorkerInputsTest.java
- src/test/java/org/apache/druid/msq/querykit/BroadcastJoinSegmentMapFnProcessorTest.java
- src/test/java/org/apache/druid/msq/querykit/FrameProcessorTestBase.java
- src/test/java/org/apache/druid/msq/querykit/RestrictedInputNumberDataSourceTest.java
- src/test/java/org/apache/druid/msq/querykit/WindowOperatorQueryFrameProcessorTest.java
- src/test/java/org/apache/druid/msq/querykit/common/SortMergeJoinFrameProcessorTest.java
- src/test/java/org/apache/druid/msq/querykit/results/QueryResultsFrameProcessorTest.java
- src/test/java/org/apache/druid/msq/querykit/scan/ScanQueryFrameProcessorTest.java
- src/test/java/org/apache/druid/msq/rpc/BaseWorkerClientImplTest.java
- src/test/java/org/apache/druid/msq/shuffle/output/ChannelStageOutputReaderTest.java
- src/test/java/org/apache/druid/msq/statistics/ByteRowKeySerdeTest.java
- src/test/java/org/apache/druid/msq/statistics/ClusterByStatisticsCollectorImplTest.java
- src/test/java/org/apache/druid/msq/statistics/DelegateOrMinKeyCollectorTest.java
- src/test/java/org/apache/druid/msq/statistics/KeyCollectorTestUtils.java
- src/test/java/org/apache/druid/msq/statistics/QuantilesSketchKeyCollectorTest.java
- src/test/java/org/apache/druid/msq/statistics/serde/KeyCollectorSnapshotSerializerTest.java
- src/test/java/org/apache/druid/msq/test/AbstractMSQComponentSupplierDelegate.java
- src/test/java/org/apache/druid/msq/test/CalciteDartTest.java
- src/test/java/org/apache/druid/msq/test/CalciteMSQTestsHelper.java
- src/test/java/org/apache/druid/msq/test/DartComponentSupplier.java
- src/test/java/org/apache/druid/msq/test/MSQCalciteSelectJoinQueryTest.java
- src/test/java/org/apache/druid/msq/test/MSQTestControllerContext.java
- src/test/java/org/apache/druid/msq/test/MSQTestWorkerClient.java
- src/test/java/org/apache/druid/msq/test/MSQTestWorkerContext.java
- src/test/java/org/apache/druid/msq/test/TestDartControllerContextFactoryImpl.java
- src/test/java/org/apache/druid/msq/util/MSQFaultUtilsTest.java
- src/test/java/org/apache/druid/sql/avatica/DartDruidMeta.java
