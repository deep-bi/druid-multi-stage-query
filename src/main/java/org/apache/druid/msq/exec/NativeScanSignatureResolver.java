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
import com.google.inject.Injector;
import com.google.inject.Key;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.indexing.common.actions.RetrieveUsedSegmentsAction;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.msq.indexing.MSQSpec;
import org.apache.druid.msq.querykit.DataSegmentProvider;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Resolves scan query signatures.
 */
class NativeScanSignatureResolver
{
  private static final Logger log = new Logger(NativeScanSignatureResolver.class);

  private final ControllerContext controllerContext;
  private final ObjectMapper jsonMapper;

  NativeScanSignatureResolver(final ControllerContext controllerContext)
  {
    this.controllerContext = controllerContext;
    this.jsonMapper = controllerContext.jsonMapper();
  }

  public MSQSpec maybeAddScanSignature(final MSQSpec querySpec)
  {
    final Query<?> query = querySpec.getQuery();
    if (!(query instanceof ScanQuery)) {
      return querySpec;
    }

    final ScanQuery scanQuery = (ScanQuery) query;
    if (scanQuery.context().get(DruidQuery.CTX_SCAN_SIGNATURE) != null) {
      return querySpec;
    }

    final RowSignature scanSignature = buildScanSignature(scanQuery);
    try {
      return querySpec.withOverriddenContext(
          ImmutableMap.of(
              DruidQuery.CTX_SCAN_SIGNATURE,
              jsonMapper.writeValueAsString(scanSignature)
          )
      );
    }
    catch (JsonProcessingException e) {
      throw DruidException.defensive().build(e, "Unable to serialize auto-generated scan signature");
    }
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
      final List<String> unresolvedColumns
  )
  {
    if (scanQuery.getDataSource() instanceof InlineDataSource) {
      return ((InlineDataSource) scanQuery.getDataSource()).getRowSignature();
    }

    if (scanQuery.getDataSource() instanceof ExternalDataSource) {
      return ((ExternalDataSource) scanQuery.getDataSource()).getSignature();
    }

    if (scanQuery.getDataSource() instanceof TableDataSource) {
      return getTableDataSourceSignature(
          ImmutableMap.of(
              ((TableDataSource) scanQuery.getDataSource()).getName(),
              scanQuery.getQuerySegmentSpec().getIntervals()
          ),
          unresolvedColumns
      );
    }

    if (scanQuery.getDataSource() instanceof UnionDataSource) {
      final Map<String, List<Interval>> tablesAndIntervals = new LinkedHashMap<>();
      for (final TableDataSource tableDataSource : ((UnionDataSource) scanQuery.getDataSource()).getDataSourcesAsTableDataSources()) {
        tablesAndIntervals.put(tableDataSource.getName(), scanQuery.getQuerySegmentSpec().getIntervals());
      }
      return getTableDataSourceSignature(tablesAndIntervals, unresolvedColumns);
    }

    throw userScanSignatureException(
        "Unable to auto-generate [%s] for dataSource type [%s]. Please provide [%s] in the query context.",
        DruidQuery.CTX_SCAN_SIGNATURE,
        scanQuery.getDataSource().getClass().getSimpleName(),
        DruidQuery.CTX_SCAN_SIGNATURE
    );
  }

  private RowSignature getTableDataSourceSignature(
      final Map<String, List<Interval>> tablesAndIntervals,
      final List<String> unresolvedColumns
  )
  {
    final LinkedHashMap<SegmentId, DataSegment> candidateSegments = new LinkedHashMap<>();

    for (final Map.Entry<String, List<Interval>> entry : tablesAndIntervals.entrySet()) {
      addCandidateSegments(candidateSegments, entry.getKey(), entry.getValue());
    }

    if (candidateSegments.isEmpty()) {
      throw userScanSignatureException(
          "Unable to auto-generate [%s] because no matching segments were found. Please provide [%s] in the query context.",
          DruidQuery.CTX_SCAN_SIGNATURE,
          DruidQuery.CTX_SCAN_SIGNATURE
      );
    }

    final RowSignature.Builder signatureBuilder = RowSignature.builder();
    final List<String> remainingColumns = new ArrayList<>(unresolvedColumns);
    Throwable lastLoadFailure = null;
    boolean loadedAnySegment = false;

    for (final DataSegment segment : candidateSegments.values()) {
      final RowSignature segmentSignature;
      try {
        segmentSignature = loadSegmentSignature(segment);
        loadedAnySegment = true;
      }
      catch (Exception e) {
        lastLoadFailure = e;
        log.warn(
            e,
            "Unable to inspect segment[%s] while auto-generating [%s]; trying another matching segment.",
            segment.getId(),
            DruidQuery.CTX_SCAN_SIGNATURE
        );
        continue;
      }

      final List<String> newlyResolvedColumns = new ArrayList<>();
      for (final String column : remainingColumns) {
        final Optional<ColumnType> type = segmentSignature.getColumnType(column);
        if (type.isPresent()) {
          signatureBuilder.add(column, type.get());
          newlyResolvedColumns.add(column);
        }
      }

      remainingColumns.removeAll(newlyResolvedColumns);
      if (remainingColumns.isEmpty()) {
        break;
      }
    }

    if (!loadedAnySegment) {
      throw DruidException.forPersona(DruidException.Persona.DEVELOPER)
                          .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                          .build(
                              lastLoadFailure,
                              "Unable to auto-generate [%s] because matching segments could not be loaded.",
                              DruidQuery.CTX_SCAN_SIGNATURE
                          );
    }

    return signatureBuilder.build();
  }

  private void addCandidateSegments(
      final LinkedHashMap<SegmentId, DataSegment> candidateSegments,
      final String dataSource,
      final List<Interval> intervals
  )
  {
    final Collection<DataSegment> publishedUsedSegments;
    try {
      if (intervals.isEmpty()) {
        publishedUsedSegments = Collections.emptyList();
      } else {
        publishedUsedSegments = controllerContext.taskActionClient().submit(
            new RetrieveUsedSegmentsAction(dataSource, intervals)
        );
      }
    }
    catch (IOException e) {
      throw DruidException.defensive()
                          .build(e, "Unable to inspect matching segments for dataSource[%s].", dataSource);
    }

    for (final DataSegment segment : publishedUsedSegments) {
      candidateSegments.put(segment.getId(), segment);
    }
  }

  private RowSignature loadSegmentSignature(final DataSegment dataSegment) throws IOException
  {
    final Injector injector = controllerContext.injector();
    if (injector.getExistingBinding(Key.get(DataSegmentProvider.class)) != null) {
      final DataSegmentProvider dataSegmentProvider = injector.getInstance(DataSegmentProvider.class);
      try (ResourceHolder<Segment> segmentHolder = dataSegmentProvider.fetchSegment(
          dataSegment.getId(),
          new ChannelCounters(),
          false
      ).get()) {
        return segmentHolder.get().asStorageAdapter().getRowSignature();
      }
    }

    final File temporaryDirectory = FileUtils.createTempDir("scan-signature");
    final SegmentCacheManager segmentCacheManager = new SegmentCacheManagerFactory(jsonMapper).manufacturate(temporaryDirectory);
    try {
      if (!segmentCacheManager.reserve(dataSegment)) {
        throw DruidException.defensive()
                            .build("Could not reserve a local cache location for segment[%s].", dataSegment.getId());
      }

      final File segmentDirectory = segmentCacheManager.getSegmentFiles(dataSegment);
      final IndexIO indexIO = injector.getInstance(IndexIO.class);

      try (
          QueryableIndex index = indexIO.loadIndex(segmentDirectory);
          QueryableIndexSegment segment = new QueryableIndexSegment(index, dataSegment.getId())
      ) {
        return segment.asStorageAdapter().getRowSignature();
      }
      finally {
        try {
          segmentCacheManager.cleanup(dataSegment);
        }
        catch (Exception e) {
          log.warn(e, "Unable to clean up local cache files for segment[%s].", dataSegment.getId());
        }
      }
    }
    catch (IOException e) {
      throw e;
    }
    catch (Exception e) {
      throw new IOException(e);
    }
    finally {
      try {
        FileUtils.deleteDirectory(temporaryDirectory);
      }
      catch (IOException e) {
        log.warn(e, "Unable to delete temporary directory[%s] used for scan signature inference.", temporaryDirectory);
      }
    }
  }

  private static DruidException userScanSignatureException(final String format, final Object... arguments)
  {
    return DruidException.forPersona(DruidException.Persona.USER)
                         .ofCategory(DruidException.Category.INVALID_INPUT)
                         .build(format, arguments);
  }
}
