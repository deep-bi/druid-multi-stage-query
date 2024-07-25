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

package org.apache.druid.msq.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.client.indexing.TaskPayloadResponse;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.NotFound;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.processor.FrameProcessors;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.msq.indexing.MSQNativeControllerTask;
import org.apache.druid.msq.indexing.error.MSQErrorReport;
import org.apache.druid.msq.indexing.error.MSQFault;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.nql.NativeStatementResult;
import org.apache.druid.msq.sql.StatementState;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.ColumnMappings;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class NativeStatementResourceHelper extends AbstractResourceHelper
{

  public static Map<String, ColumnType> getColumnTypes(RowSignature signature)
  {
    return signature.getColumnNames().stream()
                    .filter(name -> signature.getColumnType(name).isPresent())
                    .collect(Collectors.toMap(
                        name -> name,
                        name -> signature.getColumnType(name).get()
                    ));
  }

  public static Optional<NativeStatementResult> getExceptionPayload(
      String queryId,
      TaskStatusResponse taskResponse,
      TaskStatusPlus statusPlus,
      StatementState statementState,
      TaskReport.ReportMap msqPayload,
      ObjectMapper jsonMapper
  )
  {
    final MSQErrorReport exceptionDetails = getQueryExceptionDetails(getPayload(msqPayload));
    final MSQFault fault = exceptionDetails == null ? null : exceptionDetails.getFault();
    if (exceptionDetails == null || fault == null) {
      return Optional.of(new NativeStatementResult(
          queryId,
          statementState,
          taskResponse.getStatus().getCreatedTime(),
          null,
          taskResponse.getStatus().getDuration(),
          null,
          DruidException.forPersona(DruidException.Persona.DEVELOPER)
                        .ofCategory(DruidException.Category.UNCATEGORIZED)
                        .build("%s", taskResponse.getStatus().getErrorMsg())
                        .toErrorResponse()
      ));
    }

    final String errorMessage = fault.getErrorMessage() == null ? statusPlus.getErrorMsg() : fault.getErrorMessage();
    final String errorCode = fault.getErrorCode() == null ? "unknown" : fault.getErrorCode();

    final Map<String, String> exceptionContext = buildExceptionContext(fault, jsonMapper);
    return Optional.of(new NativeStatementResult(
        queryId,
        statementState,
        taskResponse.getStatus().getCreatedTime(),
        null,
        taskResponse.getStatus().getDuration(),
        null,
        DruidException.fromFailure(new DruidException.Failure(errorCode)
        {
          @Override
          protected DruidException makeException(DruidException.DruidExceptionBuilder bob)
          {
            DruidException ex = bob.forPersona(DruidException.Persona.USER)
                                   .ofCategory(DruidException.Category.UNCATEGORIZED)
                                   .build(errorMessage);
            ex.withContext(exceptionContext);
            return ex;
          }
        }).toErrorResponse()
    ));
  }


  public static Sequence<Object[]> getResultSequence(
      StageDefinition finalStage,
      Frame frame,
      ColumnMappings columnMappings
  )
  {
    final Cursor cursor = FrameProcessors.makeCursor(frame, finalStage.getFrameReader());

    final ColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();
    @SuppressWarnings("rawtypes")
    final List<ColumnValueSelector> selectors = columnMappings.getMappings()
                                                              .stream()
                                                              .map(mapping -> columnSelectorFactory.makeColumnValueSelector(
                                                                  mapping.getQueryColumn()))
                                                              .collect(Collectors.toList());

    Iterable<Object[]> retVal = () -> new Iterator<Object[]>()
    {
      @Override
      public boolean hasNext()
      {
        return !cursor.isDone();
      }

      @Override
      public Object[] next()
      {
        final Object[] row = new Object[columnMappings.size()];
        for (int i = 0; i < row.length; i++) {
          final Object value = selectors.get(i).getObject();
          row[i] = value;
        }

        cursor.advance();
        return row;
      }
    };
    return Sequences.simple(retVal);
  }

  public static void isMSQPayload(TaskPayloadResponse taskPayloadResponse, String queryId) throws DruidException
  {
    if (taskPayloadResponse == null || taskPayloadResponse.getPayload() == null) {
      throw NotFound.exception("Query[%s] not found", queryId);
    }

    if (!(taskPayloadResponse.getPayload() instanceof MSQNativeControllerTask)) {
      throw NotFound.exception("Query[%s] not found", queryId);
    }
  }
}
