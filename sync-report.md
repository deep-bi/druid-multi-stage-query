# MSQ Sync Report

## [IMPORTANT] Remember to update pom.xml and README.md manually

## Manual port required (owned & changed)
- src/main/java/org/apache/druid/msq/exec/ControllerImpl.java +
- src/main/java/org/apache/druid/msq/indexing/IndexerControllerContext.java + 
- src/main/java/org/apache/druid/msq/indexing/MSQControllerTask.java +
- src/main/java/org/apache/druid/msq/sql/resources/SqlStatementResource.java +
- src/test/java/org/apache/druid/msq/quidem/MSQQuidemTest.java + 
- src/test/java/org/apache/druid/msq/sql/resources/SqlMSQStatementResourcePostTest.java +
- src/test/java/org/apache/druid/msq/sql/resources/SqlStatementResourceTest.java +
- src/test/java/org/apache/druid/msq/test/CalciteSelectQueryMSQTest.java + 
- src/test/java/org/apache/druid/msq/test/MSQTestBase.java + 
- src/test/java/org/apache/druid/msq/test/MSQTestOverlordServiceClient.java + 

### Method-level diffs
### src/main/java/org/apache/druid/msq/exec/ControllerImpl.java
- Signature changes: none

### src/main/java/org/apache/druid/msq/indexing/IndexerControllerContext.java
- Added methods:
  - `org.apache.druid.msq.indexing.IndexerControllerContext#IndexerControllerContext(TaskLockType, String, QueryContext, Map, ServiceMetricEvent.Builder, TaskToolbox, Injector, ServiceClientFactory, OverlordClient)`
- Removed methods:
  - `org.apache.druid.msq.indexing.IndexerControllerContext#IndexerControllerContext(MSQControllerTask, TaskToolbox, Injector, ServiceClientFactory, OverlordClient)`

### src/main/java/org/apache/druid/msq/indexing/MSQControllerTask.java
- Added methods:
  - `org.apache.druid.msq.indexing.MSQControllerTask#isExport(MSQDestination)`
  - `org.apache.druid.msq.indexing.MSQControllerTask#isIngestion(MSQDestination)`
  - `org.apache.druid.msq.indexing.MSQControllerTask#isReplaceInputDataSourceTask(Query, MSQDestination)`
  - `org.apache.druid.msq.indexing.MSQControllerTask#writeFinalResultsToTaskReport(MSQDestination)`
  - `org.apache.druid.msq.indexing.MSQControllerTask#writeFinalStageResultsToDurableStorage(MSQDestination)`
- Removed methods:
  - `org.apache.druid.msq.indexing.MSQControllerTask#isExport(MSQSpec)`
  - `org.apache.druid.msq.indexing.MSQControllerTask#isReplaceInputDataSourceTask(MSQSpec)`
  - `org.apache.druid.msq.indexing.MSQControllerTask#writeFinalResultsToTaskReport(MSQSpec)`
  - `org.apache.druid.msq.indexing.MSQControllerTask#writeFinalStageResultsToDurableStorage(MSQSpec)`

### src/main/java/org/apache/druid/msq/sql/resources/SqlStatementResource.java
- Added methods:
  - `org.apache.druid.msq.sql.resources.SqlStatementResource#doGetResults(String, Long, String, String, HttpServletRequest)`
- Removed methods:
  - `org.apache.druid.msq.sql.resources.SqlStatementResource#doGetResults(String, Long, String, HttpServletRequest)`

### src/test/java/org/apache/druid/msq/quidem/MSQQuidemTest.java
- Signature changes: none

### src/test/java/org/apache/druid/msq/sql/resources/SqlMSQStatementResourcePostTest.java
- Signature changes: none

### src/test/java/org/apache/druid/msq/sql/resources/SqlStatementResourceTest.java
- Added methods:
  - `org.apache.druid.msq.sql.resources.SqlStatementResourceTest#testDownloadResultsAsFile()`
  - `org.apache.druid.msq.sql.resources.SqlStatementResourceTest#testValidFilename()`

### src/test/java/org/apache/druid/msq/test/CalciteSelectQueryMSQTest.java
- Removed methods:
  - `org.apache.druid.msq.test.CalciteSelectQueryMSQTest#testFilterParseLongNullable()`

### src/test/java/org/apache/druid/msq/test/MSQTestBase.java
- Added methods:
  - `org.apache.druid.msq.test.MSQTestBase.IngestTester#setExpectedProjections(List)`

### src/test/java/org/apache/druid/msq/test/MSQTestOverlordServiceClient.java
- Signature changes: none

## Auto-applied (changed but not owned)
- src/main/java/org/apache/druid/msq/dart/controller/DartControllerContext.java
- src/main/java/org/apache/druid/msq/dart/controller/DartControllerContextFactoryImpl.java
- src/main/java/org/apache/druid/msq/dart/controller/DartTableInputSpecSlicer.java
- src/main/java/org/apache/druid/msq/dart/worker/DartDataSegmentProvider.java
- src/main/java/org/apache/druid/msq/dart/worker/DartWorkerClient.java
- src/main/java/org/apache/druid/msq/dart/worker/DartWorkerFactoryImpl.java
- src/main/java/org/apache/druid/msq/dart/worker/DartWorkerRetryPolicy.java
- src/main/java/org/apache/druid/msq/exec/ExceptionWrappingWorkerClient.java
- src/main/java/org/apache/druid/msq/exec/RunWorkOrder.java
- src/main/java/org/apache/druid/msq/exec/WorkerSketchFetcher.java
- src/main/java/org/apache/druid/msq/indexing/MSQCompactionRunner.java
- src/main/java/org/apache/druid/msq/indexing/MSQSpec.java
- src/main/java/org/apache/druid/msq/indexing/MSQWorkerTaskLauncher.java
- src/main/java/org/apache/druid/msq/indexing/destination/DataSourceMSQDestination.java
- src/main/java/org/apache/druid/msq/indexing/destination/SegmentGenerationUtils.java
- src/main/java/org/apache/druid/msq/indexing/error/WorkerFailedFault.java
- src/main/java/org/apache/druid/msq/indexing/error/WorkerRpcFailedFault.java
- src/main/java/org/apache/druid/msq/indexing/processor/SegmentGeneratorFrameProcessorFactory.java
- src/main/java/org/apache/druid/msq/querykit/BaseLeafFrameProcessorFactory.java
- src/main/java/org/apache/druid/msq/querykit/BroadcastJoinSegmentMapFnProcessor.java
- src/main/java/org/apache/druid/msq/querykit/DataSourcePlan.java
- src/main/java/org/apache/druid/msq/querykit/InputNumberDataSource.java
- src/main/java/org/apache/druid/msq/querykit/ShuffleSpecFactories.java
- src/main/java/org/apache/druid/msq/querykit/SimpleSegmentMapFnProcessor.java
- src/main/java/org/apache/druid/msq/querykit/groupby/GroupByPostShuffleFrameProcessor.java
- src/main/java/org/apache/druid/msq/querykit/groupby/GroupByQueryKit.java
- src/main/java/org/apache/druid/msq/querykit/scan/ScanQueryFrameProcessor.java
- src/main/java/org/apache/druid/msq/sql/MSQTaskQueryMaker.java
- src/main/java/org/apache/druid/msq/util/MSQTaskQueryMakerUtils.java
- src/test/java/org/apache/druid/msq/dart/controller/DartControllerContextTest.java
- src/test/java/org/apache/druid/msq/dart/controller/DartTableInputSpecSlicerTest.java
- src/test/java/org/apache/druid/msq/dart/controller/DartWorkerManagerTest.java
- src/test/java/org/apache/druid/msq/exec/MSQExportTest.java
- src/test/java/org/apache/druid/msq/exec/MSQInsertTest.java
- src/test/java/org/apache/druid/msq/exec/MSQParseExceptionsTest.java
- src/test/java/org/apache/druid/msq/exec/MSQReplaceTest.java
- src/test/java/org/apache/druid/msq/exec/MSQSelectTest.java
- src/test/java/org/apache/druid/msq/exec/MSQTasksTest.java
- src/test/java/org/apache/druid/msq/indexing/MSQCompactionRunnerTest.java
- src/test/java/org/apache/druid/msq/indexing/MSQControllerTaskTest.java
- src/test/java/org/apache/druid/msq/indexing/MSQTuningConfigTest.java
- src/test/java/org/apache/druid/msq/indexing/MSQWorkerTaskLauncherTest.java
- src/test/java/org/apache/druid/msq/indexing/destination/DataSourceMSQDestinationTest.java
- src/test/java/org/apache/druid/msq/indexing/error/MSQFaultSerdeTest.java
- src/test/java/org/apache/druid/msq/test/AbstractMSQComponentSupplierDelegate.java
- src/test/java/org/apache/druid/msq/test/CalciteMSQTestsHelper.java
- src/test/java/org/apache/druid/msq/test/MSQTestControllerContext.java
- src/test/java/org/apache/druid/msq/test/MSQTestWorkerClient.java
- src/test/java/org/apache/druid/msq/test/MSQTestWorkerContext.java
- src/test/java/org/apache/druid/msq/util/DimensionSchemaUtilsTest.java
- src/test/java/org/apache/druid/msq/util/SqlStatementResourceHelperTest.java
