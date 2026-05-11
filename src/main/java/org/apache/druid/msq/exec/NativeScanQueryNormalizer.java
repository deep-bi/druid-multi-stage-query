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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.error.DruidException;
import org.apache.druid.msq.querykit.scan.ScanQueryKit;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.FilteredDataSource;
import org.apache.druid.query.FrameBasedInlineDataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.UnnestDataSource;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.operator.WindowOperatorQuery;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.metadata.DataSourceInformation;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.rel.DruidQuery;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Normalizes scan queries for native MSQ execution.
 */
public class NativeScanQueryNormalizer
{
  private final ObjectMapper jsonMapper;
  private final CoordinatorClient coordinatorClient;

  public NativeScanQueryNormalizer(
      final ObjectMapper jsonMapper,
      final CoordinatorClient coordinatorClient
  )
  {
    this.jsonMapper = jsonMapper;
    this.coordinatorClient = coordinatorClient;
  }

  public ScanQuery normalize(final ScanQuery query)
  {
    return normalizeCurrentScan(withNormalizedSubqueryScans(query));
  }

  private ScanQuery normalizeCurrentScan(final ScanQuery query)
  {
    ScanQuery scanQuery = query;
    RowSignature dataSourceSignature = null;

    if (!hasExplicitColumns(scanQuery)) {
      dataSourceSignature = getDataSourceSignature(scanQuery.getDataSource());
      scanQuery = withAllColumns(scanQuery, dataSourceSignature);
    }

    if (hasColumnTypes(scanQuery) || hasScanSignature(scanQuery)) {
      return scanQuery;
    }

    if (dataSourceSignature == null) {
      dataSourceSignature = getDataSourceSignature(scanQuery.getDataSource());
    }

    final RowSignature scanSignature = buildScanSignature(scanQuery, dataSourceSignature);
    return Druids.ScanQueryBuilder.copy(scanQuery)
                                  .columnTypes(scanSignature.getColumnTypes())
                                  .build();
  }

  private ScanQuery withNormalizedSubqueryScans(final ScanQuery scanQuery)
  {
    final DataSource dataSource = scanQuery.getDataSource();
    final DataSource normalizedDataSource = normalizeSubqueryScansInDataSource(dataSource);

    if (normalizedDataSource == dataSource) {
      return scanQuery;
    }

    return Druids.ScanQueryBuilder.copy(scanQuery)
                                  .dataSource(normalizedDataSource)
                                  .build();
  }

  private DataSource normalizeSubqueryScansInDataSource(final DataSource dataSource)
  {
    if (dataSource instanceof QueryDataSource) {
      return normalizeQueryDataSource((QueryDataSource) dataSource);
    }

    if (canContainQueryDataSources(dataSource)) {
      return normalizeSubqueryScansInChildren(dataSource);
    }

    return dataSource;
  }

  private boolean canContainQueryDataSources(final DataSource dataSource)
  {
    return dataSource instanceof FilteredDataSource
           || dataSource instanceof JoinDataSource
           || dataSource instanceof UnionDataSource
           || dataSource instanceof UnnestDataSource;
  }

  private DataSource normalizeSubqueryScansInChildren(final DataSource dataSource)
  {
    final List<DataSource> children = dataSource.getChildren();
    if (children.isEmpty()) {
      return dataSource;
    }

    final List<DataSource> normalizedChildren = new ArrayList<>(children.size());
    boolean changed = false;

    for (final DataSource child : children) {
      final DataSource normalizedChild = normalizeSubqueryScansInDataSource(child);
      normalizedChildren.add(normalizedChild);
      changed |= normalizedChild != child;
    }

    return changed ? dataSource.withChildren(normalizedChildren) : dataSource;
  }

  private DataSource normalizeQueryDataSource(final QueryDataSource dataSource)
  {
    final Query<?> query = dataSource.getQuery();
    if (!(query instanceof ScanQuery)) {
      return normalizeSubqueryScansInChildren(dataSource);
    }

    final ScanQuery normalizedQuery = normalize((ScanQuery) query);
    return normalizedQuery == query ? dataSource : new QueryDataSource(normalizedQuery);
  }

  private boolean hasColumnTypes(final ScanQuery scanQuery)
  {
    return scanQuery.getColumnTypes() != null;
  }

  private boolean hasScanSignature(final ScanQuery scanQuery)
  {
    return scanQuery.context().get(DruidQuery.CTX_SCAN_SIGNATURE) != null;
  }

  private RowSignature buildScanSignature(
      final ScanQuery scanQuery,
      final RowSignature dataSourceSignature
  )
  {
    final List<String> outputColumns = scanQuery.getColumns();
    final RowSignature allKnownSignature = buildCombinedSignature(scanQuery, dataSourceSignature);
    final RowSignature.Builder outputSignatureBuilder = RowSignature.builder();

    for (final String column : outputColumns) {
      final ColumnType type = allKnownSignature.getColumnType(column).orElse(null);
      if (type == null) {
        throw userColumnTypesException(
            "Unable to auto-generate columnTypes for column [%s]. Please provide columnTypes in the query.",
            column
        );
      }

      outputSignatureBuilder.add(column, type);
    }

    return outputSignatureBuilder.build();
  }

  private boolean hasExplicitColumns(final ScanQuery scanQuery)
  {
    final List<String> columns = scanQuery.getColumns();
    return columns != null && !columns.isEmpty();
  }

  private ScanQuery withAllColumns(final ScanQuery scanQuery, final RowSignature dataSourceSignature)
  {
    return Druids.ScanQueryBuilder.copy(scanQuery)
                                  .columns(getAllColumns(scanQuery, dataSourceSignature))
                                  .build();
  }

  private List<String> getAllColumns(final ScanQuery scanQuery, final RowSignature dataSourceSignature)
  {
    final List<String> columns = new ArrayList<>(dataSourceSignature.getColumnNames());

    for (final VirtualColumn virtualColumn : scanQuery.getVirtualColumns().getVirtualColumns()) {
      if (!columns.contains(virtualColumn.getOutputName())) {
        columns.add(virtualColumn.getOutputName());
      }
    }

    return columns;
  }

  private RowSignature buildCombinedSignature(
      final ScanQuery scanQuery,
      final RowSignature dataSourceSignature
  )
  {
    final VirtualColumns virtualColumns = scanQuery.getVirtualColumns();
    final RowSignature.Builder builder = RowSignature.builder().addAll(dataSourceSignature);

    final List<String> pending = Arrays.stream(virtualColumns.getVirtualColumns())
                                       .map(VirtualColumn::getOutputName)
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

  private RowSignature getDataSourceSignature(final DataSource dataSource)
  {
    if (dataSource instanceof InlineDataSource) {
      return ((InlineDataSource) dataSource).getRowSignature();
    }

    if (dataSource instanceof FrameBasedInlineDataSource) {
      return ((FrameBasedInlineDataSource) dataSource).getRowSignature();
    }

    if (dataSource instanceof ExternalDataSource) {
      return ((ExternalDataSource) dataSource).getSignature();
    }

    if (dataSource instanceof FilteredDataSource) {
      return getDataSourceSignature(((FilteredDataSource) dataSource).getBase());
    }

    if (dataSource instanceof UnnestDataSource) {
      return getUnnestDataSourceSignature((UnnestDataSource) dataSource);
    }

    if (dataSource instanceof JoinDataSource) {
      return getJoinDataSourceSignature((JoinDataSource) dataSource);
    }

    if (dataSource instanceof QueryDataSource) {
      return getQueryDataSourceSignature((QueryDataSource) dataSource);
    }

    if (dataSource instanceof UnionDataSource) {
      return getDataSourceSignature(((UnionDataSource) dataSource).getDataSources().get(0));
    }

    return getDataSourceSignatureFromCentralizedSchema(dataSource);
  }

  private RowSignature getUnnestDataSourceSignature(final UnnestDataSource dataSource)
  {
    final RowSignature baseSignature = getDataSourceSignature(dataSource.getBase());
    final VirtualColumn virtualColumn = dataSource.getVirtualColumn();
    final ColumnCapabilities capabilities = virtualColumn.capabilities(baseSignature, virtualColumn.getOutputName());
    return RowSignature.builder()
                       .addAll(baseSignature)
                       .add(
                           virtualColumn.getOutputName(),
                           capabilities == null ? null : capabilities.toColumnType()
                       )
                       .build();
  }

  private RowSignature getJoinDataSourceSignature(final JoinDataSource dataSource)
  {
    final RowSignature leftSignature = getDataSourceSignature(dataSource.getLeft());
    final RowSignature rightSignature = getDataSourceSignature(dataSource.getRight());
    final RowSignature.Builder builder = RowSignature.builder().addAll(leftSignature);

    for (final String column : rightSignature.getColumnNames()) {
      builder.add(dataSource.getRightPrefix() + column, rightSignature.getColumnType(column).orElse(null));
    }

    return builder.build();
  }

  private RowSignature getQueryDataSourceSignature(final QueryDataSource dataSource)
  {
    final Query<?> query = dataSource.getQuery();

    if (query instanceof ScanQuery) {
      return ScanQueryKit.getAndValidateSignature(normalize((ScanQuery) query), jsonMapper);
    }

    if (query instanceof GroupByQuery) {
      final GroupByQuery groupByQuery = (GroupByQuery) query;
      return groupByQuery.getResultRowSignature(
          groupByQuery.context().isFinalize(true) ? RowSignature.Finalization.YES : RowSignature.Finalization.NO
      );
    }

    if (query instanceof TimeseriesQuery) {
      final TimeseriesQuery timeseriesQuery = (TimeseriesQuery) query;
      return timeseriesQuery.getResultSignature(
          timeseriesQuery.context().isFinalize(true) ? RowSignature.Finalization.YES : RowSignature.Finalization.NO
      );
    }

    if (query instanceof TopNQuery) {
      final TopNQuery topNQuery = (TopNQuery) query;
      return topNQuery.getResultSignature(
          topNQuery.context().isFinalize(true) ? RowSignature.Finalization.YES : RowSignature.Finalization.NO
      );
    }

    if (query instanceof WindowOperatorQuery) {
      return ((WindowOperatorQuery) query).getRowSignature();
    }

    throw userColumnTypesException(
        "Unable to auto-generate columnTypes for query datasource [%s]. Please provide columnTypes in the query.",
        dataSource
    );
  }

  private RowSignature getDataSourceSignatureFromCentralizedSchema(final DataSource dataSource)
  {
    if (!(dataSource instanceof TableDataSource)) {
      throw userColumnTypesException(
          "Unable to auto-generate columnTypes for datasource [%s]. Please provide columnTypes in the query.",
          dataSource
      );
    }

    final String dataSourceName = ((TableDataSource) dataSource).getName();
    final List<DataSourceInformation> dataSourceInformation = FutureUtils.getUnchecked(
        coordinatorClient.fetchDataSourceInformation(Collections.singleton(dataSourceName)),
        true
    );

    for (final DataSourceInformation information : dataSourceInformation) {
      if (dataSourceName.equals(information.getDataSource()) && information.getRowSignature() != null) {
        return information.getRowSignature();
      }
    }

    throw userColumnTypesException(
        "Unable to auto-generate columnTypes because datasource [%s] centralized schema is not available. "
        + "Please provide columnTypes in the query.",
        dataSourceName
    );
  }

  private static DruidException userColumnTypesException(final String format, final Object... arguments)
  {
    return DruidException.forPersona(DruidException.Persona.USER)
                         .ofCategory(DruidException.Category.INVALID_INPUT)
                         .build(format, arguments);
  }
}
