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
import com.google.common.collect.ImmutableMap;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.metadata.metadata.AggregatorMergeStrategy;
import org.apache.druid.query.metadata.metadata.ColumnAnalysis;
import org.apache.druid.query.metadata.metadata.ListColumnIncluderator;
import org.apache.druid.query.metadata.metadata.SegmentAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.QueryLifecycle;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.rel.DruidQuery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Resolves scan query signatures.
 */
public class NativeScanSignatureResolver
{
  private static final Logger log = new Logger(NativeScanSignatureResolver.class);

  private final ObjectMapper jsonMapper;
  private final QueryLifecycleFactory lifecycleFactory;
  private final AuthenticationResult authenticationResult;

  public NativeScanSignatureResolver(
      final ObjectMapper jsonMapper,
      final QueryLifecycleFactory lifecycleFactory,
      final AuthenticationResult authenticationResult
  )
  {
    this.jsonMapper = jsonMapper;
    this.lifecycleFactory = lifecycleFactory;
    this.authenticationResult = authenticationResult;
  }

  public Query<?> maybeAddScanSignature(final Query<?> query) throws JsonProcessingException
  {
    if (!(query instanceof ScanQuery) || query.context().get(DruidQuery.CTX_SCAN_SIGNATURE) != null) {
      return query;
    }

    final ScanQuery scanQuery = (ScanQuery) query;

    return query.withOverriddenContext(getScanSignatureContextOverride(scanQuery));
  }

  private Map<String, Object> getScanSignatureContextOverride(final ScanQuery scanQuery)
      throws JsonProcessingException
  {
    return ImmutableMap.of(
        DruidQuery.CTX_SCAN_SIGNATURE,
        jsonMapper.writeValueAsString(buildScanSignature(scanQuery))
    );
  }

  private RowSignature buildScanSignature(final ScanQuery scanQuery)
  {
    final List<String> outputColumns = getOutputColumns(scanQuery);
    final RowSignature dataSourceSignature = getDataSourceSignature(
        scanQuery,
        getRequiredDataSourceColumns(scanQuery, outputColumns)
    );

    final RowSignature allKnownSignature = buildCombinedSignature(scanQuery, outputColumns, dataSourceSignature);
    final RowSignature.Builder outputSignatureBuilder = RowSignature.builder();

    for (final String column : outputColumns) {
      final ColumnType type = allKnownSignature.getColumnType(column).orElse(null);
      if (type == null) {
        throw userScanSignatureException(
            "Unable to auto-generate [%s] for column [%s]. Please provide [%s] in the query context.",
            DruidQuery.CTX_SCAN_SIGNATURE,
            column,
            DruidQuery.CTX_SCAN_SIGNATURE
        );
      }

      outputSignatureBuilder.add(column, type);
    }

    return outputSignatureBuilder.build();
  }

  private List<String> getOutputColumns(final ScanQuery scanQuery)
  {
    final List<String> columns = scanQuery.getColumns();
    if (columns == null || columns.isEmpty()) {
      throw userScanSignatureException(
          "Unable to auto-generate [%s] for scan queries without explicit [columns]. "
          + "Please provide [%s] in the query context.",
          DruidQuery.CTX_SCAN_SIGNATURE,
          DruidQuery.CTX_SCAN_SIGNATURE
      );
    }

    return columns;
  }

  private List<String> getRequiredDataSourceColumns(final ScanQuery scanQuery, final List<String> outputColumns)
  {
    final List<String> requiredColumns = new ArrayList<>();

    for (final String column : outputColumns) {
      addRequiredDataSourceColumns(scanQuery, column, requiredColumns);
    }

    return requiredColumns;
  }

  private void addRequiredDataSourceColumns(
      final ScanQuery scanQuery,
      final String column,
      final List<String> requiredColumns
  )
  {
    if (!scanQuery.getVirtualColumns().exists(column)) {
      if (!requiredColumns.contains(column)) {
        requiredColumns.add(column);
      }
      return;
    }

    final VirtualColumn virtualColumn = scanQuery.getVirtualColumns().getVirtualColumn(column);
    if (virtualColumn == null) {
      return;
    }

    for (final String requiredColumn : virtualColumn.requiredColumns()) {
      addRequiredDataSourceColumns(scanQuery, requiredColumn, requiredColumns);
    }
  }

  private RowSignature buildCombinedSignature(
      final ScanQuery scanQuery,
      final List<String> outputColumns,
      final RowSignature dataSourceSignature
  )
  {
    final VirtualColumns virtualColumns = scanQuery.getVirtualColumns();
    final RowSignature.Builder builder = RowSignature.builder().addAll(dataSourceSignature);

    final List<String> pending = outputColumns.stream()
                                              .filter(virtualColumns::exists)
                                              .collect(Collectors.toCollection(ArrayList::new));

    boolean progress;

    do {
      progress = false;
      final RowSignature current = builder.build();

      final Iterator<String> it = pending.iterator();
      while (it.hasNext()) {
        final String column = it.next();
        final VirtualColumn vc = virtualColumns.getVirtualColumn(column);

        if (vc == null) {
          it.remove();
          continue;
        }

        final ColumnCapabilities cap = vc.capabilities(current, column);
        final ColumnType type = cap == null ? null : cap.toColumnType();

        if (type != null) {
          builder.add(column, type);
          it.remove();
          progress = true;
        }
      }
    } while (!pending.isEmpty() && progress);

    return builder.build();
  }

  private RowSignature getDataSourceSignature(
      final ScanQuery scanQuery,
      final List<String> requiredDataSourceColumns
  )
  {
    if (scanQuery.getDataSource() instanceof InlineDataSource) {
      return ((InlineDataSource) scanQuery.getDataSource()).getRowSignature();
    }

    if (scanQuery.getDataSource() instanceof ExternalDataSource) {
      return ((ExternalDataSource) scanQuery.getDataSource()).getSignature();
    }

    return getDataSourceSignatureFromSegmentMetadataQuery(scanQuery, requiredDataSourceColumns);
  }

  private static DruidException userScanSignatureException(final String format, final Object... arguments)
  {
    return DruidException.forPersona(DruidException.Persona.USER)
                         .ofCategory(DruidException.Category.INVALID_INPUT)
                         .build(format, arguments);
  }

  private RowSignature getDataSourceSignatureFromSegmentMetadataQuery(
      final ScanQuery scanQuery,
      final List<String> requiredDataSourceColumns
  )
  {
    final SegmentMetadataQuery segmentMetadataQuery = new SegmentMetadataQuery(
        scanQuery.getDataSource(),
        scanQuery.getQuerySegmentSpec(),
        new ListColumnIncluderator(requiredDataSourceColumns),
        true,
        QueryContexts.override(
            scanQuery.getContext(),
            QueryContexts.BROKER_PARALLEL_MERGE_KEY,
            false
        ),
        EnumSet.noneOf(SegmentMetadataQuery.AnalysisType.class),
        false,
        null,
        AggregatorMergeStrategy.LENIENT
    );

    final QueryLifecycle lifecycle = lifecycleFactory.factorize();
    final Sequence<SegmentAnalysis> sequence =
        lifecycle.runSimple(segmentMetadataQuery, authenticationResult, Access.OK).getResults();
    Yielder<SegmentAnalysis> yielder = Yielders.each(sequence);

    try {
      if (yielder.isDone()) {
        throw userScanSignatureException(
            "Unable to auto-generate [%s] because segment metadata query returned no results. "
            + "Please provide [%s] in the query context.",
            DruidQuery.CTX_SCAN_SIGNATURE,
            DruidQuery.CTX_SCAN_SIGNATURE
        );
      }

      return segmentAnalysisToRowSignature(yielder.get());
    }
    finally {
      try {
        yielder.close();
      }
      catch (IOException e) {
        log.warn(e, "Unable to close segment metadata query results.");
      }
    }
  }

  public static RowSignature segmentAnalysisToRowSignature(final SegmentAnalysis analysis)
  {
    final RowSignature.Builder signatureBuilder = RowSignature.builder();

    for (final Map.Entry<String, ColumnAnalysis> entry : analysis.getColumns().entrySet()) {
      if (entry.getValue().isError()) {
        log.warn(
            "Scan signature generation hit a segment metadata analysis error for column [%s], selected type [%s]."
            + " If this type is incorrect, provide [%s] manually in the query context. Error: %s",
            entry.getKey(),
            entry.getValue().getTypeSignature(),
            DruidQuery.CTX_SCAN_SIGNATURE,
            entry.getValue().getErrorMessage()
        );
      }

      final ColumnType valueType = entry.getValue().getTypeSignature();
      if (valueType != null) {
        signatureBuilder.add(entry.getKey(), valueType);
      }
    }

    return signatureBuilder.build();
  }
}
