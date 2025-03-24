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

package org.apache.druid.msq.nql;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.error.ErrorResponse;
import org.apache.druid.msq.StatementResult;
import org.apache.druid.msq.counters.CounterSnapshotsTree;
import org.apache.druid.msq.indexing.error.MSQErrorReport;
import org.apache.druid.msq.indexing.report.MSQStagesReport;
import org.apache.druid.msq.sql.StatementState;
import org.apache.druid.msq.sql.entity.ResultSetInformation;
import org.apache.druid.segment.column.ColumnType;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class NativeStatementResult implements StatementResult
{
  private final String queryId;

  private final StatementState state;

  private final DateTime createdAt;

  @Nullable
  private final Map<String, ColumnType> rowSignature;

  @Nullable
  private final Long durationMs;

  @Nullable
  private final ResultSetInformation resultSetInformation;

  @Nullable
  private final ErrorResponse errorResponse;

  @Nullable
  private final MSQStagesReport stages;

  @Nullable
  private final CounterSnapshotsTree counters;

  @Nullable
  private final List<MSQErrorReport> warnings;

  public NativeStatementResult(
      String queryId,
      StatementState state,
      DateTime createdAt,
      Map<String, ColumnType> rowSignature,
      Long durationMs,
      ResultSetInformation resultSetInformation,
      ErrorResponse errorResponse
  )
  {
    this(queryId, state, createdAt, rowSignature, durationMs, resultSetInformation, errorResponse, null, null, null);
  }

  @JsonCreator
  public NativeStatementResult(
      @JsonProperty("queryId")
      String queryId,
      @JsonProperty("state")
      StatementState state,
      @JsonProperty("createdAt")
      DateTime createdAt,
      @Nullable @JsonProperty("schema")
      Map<String, ColumnType> rowSignature,
      @Nullable @JsonProperty("durationMs")
      Long durationMs,
      @Nullable @JsonProperty("result")
      ResultSetInformation resultSetInformation,
      @Nullable @JsonProperty("errorDetails")
      ErrorResponse errorResponse,
      @Nullable @JsonProperty("stages")
      MSQStagesReport stages,
      @Nullable @JsonProperty("counters")
      CounterSnapshotsTree counters,
      @Nullable @JsonProperty("warnings")
      List<MSQErrorReport> warnings

  )
  {
    this.queryId = queryId;
    this.state = state;
    this.createdAt = createdAt;
    this.rowSignature = rowSignature;
    this.durationMs = durationMs;
    this.resultSetInformation = resultSetInformation;
    this.errorResponse = errorResponse;
    this.stages = stages;
    this.counters = counters;
    this.warnings = warnings;
  }

  @JsonProperty
  public String getQueryId()
  {
    return queryId;
  }

  @JsonProperty
  public StatementState getState()
  {
    return state;
  }

  @JsonProperty
  public DateTime getCreatedAt()
  {
    return createdAt;
  }

  @JsonProperty("schema")
  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Map<String, ColumnType> getRowSignature()
  {
    return rowSignature;
  }

  @JsonProperty
  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Long getDurationMs()
  {
    return durationMs;
  }

  @JsonProperty("result")
  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public ResultSetInformation getResultSetInformation()
  {
    return resultSetInformation;
  }

  @JsonProperty("errorDetails")
  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public ErrorResponse getErrorResponse()
  {
    return errorResponse;
  }

  @JsonProperty("stages")
  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public MSQStagesReport getStages()
  {
    return stages;
  }

  @JsonProperty("counters")
  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public CounterSnapshotsTree getCounters()
  {
    return counters;
  }

  @JsonProperty("warnings")
  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<MSQErrorReport> getWarnings()
  {
    return warnings;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof NativeStatementResult)) {
      return false;
    }
    NativeStatementResult that = (NativeStatementResult) o;

    return Objects.equals(queryId, that.queryId) &&
           state == that.state &&
           Objects.equals(createdAt, that.createdAt) &&
           Objects.equals(rowSignature, that.rowSignature) &&
           Objects.equals(durationMs, that.durationMs) &&
           Objects.equals(resultSetInformation, that.resultSetInformation) &&
           Objects.equals(stages, that.stages) &&
           Objects.equals(counters, that.counters) &&
           Objects.equals(warnings, that.warnings) &&
           Objects.equals(
               errorResponse != null ? errorResponse.getAsMap() : null,
               that.errorResponse != null ? that.errorResponse.getAsMap() : null
           );
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        queryId,
        state,
        createdAt,
        rowSignature,
        durationMs,
        resultSetInformation,
        stages,
        counters,
        warnings,
        errorResponse != null ? errorResponse.getAsMap() : null
    );
  }

  @Override
  public String toString()
  {
    return "NativeStatementResult{" +
           "queryId='" + queryId + '\'' +
           ", state=" + state +
           ", createdAt=" + createdAt +
           ", rowSignature=" + rowSignature +
           ", durationMs=" + durationMs +
           ", resultSetInformation=" + resultSetInformation +
           ", errorResponse=" + (errorResponse != null ? errorResponse.getAsMap() : "{}") +
           ", stages=" + stages +
           ", counters=" + counters +
           ", warnings=" + warnings +
           '}';
  }
}
