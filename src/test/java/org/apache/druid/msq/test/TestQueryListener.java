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

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.msq.exec.QueryListener;
import org.apache.druid.msq.indexing.destination.MSQDestination;
import org.apache.druid.msq.indexing.report.MSQResultsReport;
import org.apache.druid.msq.indexing.report.MSQStatusReport;
import org.apache.druid.msq.indexing.report.MSQTaskReport;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * Listener that captures a report and makes it available through {@link #getReportMap()}.
 */
public class TestQueryListener implements QueryListener
{
  private final String taskId;
  private final MSQDestination destination;
  private final List<Object[]> results = new ArrayList<>();

  private List<MSQResultsReport.ColumnAndType> signature;
  private List<SqlTypeName> sqlTypeNames;
  private boolean resultsTruncated = true;
  private TaskReport.ReportMap reportMap;

  public TestQueryListener(final String taskId, final MSQDestination destination)
  {
    this.taskId = taskId;
    this.destination = destination;
  }

  @Override
  public boolean readResults()
  {
    return destination.getRowsInTaskReport() == MSQDestination.UNLIMITED || destination.getRowsInTaskReport() > 0;
  }

  @Override
  public void onResultsStart(List<MSQResultsReport.ColumnAndType> signature, @Nullable List<SqlTypeName> sqlTypeNames)
  {
    this.signature = signature;
    this.sqlTypeNames = sqlTypeNames;
  }

  @Override
  public boolean onResultRow(Object[] row)
  {
    if (destination.getRowsInTaskReport() == MSQDestination.UNLIMITED
        || results.size() < destination.getRowsInTaskReport()) {
      results.add(row);
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void onResultsComplete()
  {
    resultsTruncated = false;
  }

  @Override
  public void onQueryComplete(MSQTaskReportPayload report)
  {
    final MSQResultsReport resultsReport;

    if (signature != null) {
      resultsReport = new MSQResultsReport(
          signature,
          sqlTypeNames,
          results,
          resultsTruncated
      );
    } else {
      resultsReport = null;
    }

    final MSQTaskReport taskReport = new MSQTaskReport(
        taskId,
        new MSQTaskReportPayload(
            report.getStatus(),
            report.getStages(),
            report.getCounters(),
            resultsReport
        )
    );

    reportMap = TaskReport.buildTaskReports(taskReport);
  }

  public TaskReport.ReportMap getReportMap()
  {
    return Preconditions.checkNotNull(reportMap, "reportMap");
  }

  public MSQStatusReport getStatusReport()
  {
    final MSQTaskReport taskReport = (MSQTaskReport) Iterables.getOnlyElement(getReportMap().values());
    return taskReport.getPayload().getStatus();
  }
}
