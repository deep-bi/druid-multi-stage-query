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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.query.Druids;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.segment.metadata.DataSourceInformation;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class NativeScanQueryNormalizerTest
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  @BeforeClass
  public static void setupClass()
  {
    ExpressionProcessing.initializeForTests();
    NullHandling.initializeForTests();
  }

  @Test
  public void testEmptyColumnsUseAllSegmentColumnsAndVirtualColumns() throws Exception
  {
    final NativeScanQueryNormalizer normalizer = createNormalizer(dataSourceSignature());

    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource("foo")
                                  .intervals(intervals("2000/2001"))
                                  .columns(Collections.emptyList())
                                  .virtualColumns(
                                      new ExpressionVirtualColumn(
                                          "v0",
                                          "concat(dim1, 'x')",
                                          ColumnType.STRING,
                                          ExprMacroTable.nil()
                                      )
                                  )
                                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                  .build();

    final ScanQuery normalizedQuery = normalizer.normalize(query);

    Assert.assertEquals(ImmutableList.of("__time", "cnt", "dim1", "v0"), normalizedQuery.getColumns());
    Assert.assertEquals(
        ImmutableList.of(ColumnType.LONG, ColumnType.LONG, ColumnType.STRING, ColumnType.STRING),
        normalizedQuery.getColumnTypes()
    );
    Assert.assertNull(normalizedQuery.context().get(DruidQuery.CTX_SCAN_SIGNATURE));
  }

  @Test
  public void testUsesCentralizedSchemaForExplicitColumns() throws Exception
  {
    final NativeScanQueryNormalizer normalizer = createNormalizer(dataSourceSignature());

    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource("foo")
                                  .intervals(intervals("2000/2001"))
                                  .columns("cnt", "dim1")
                                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                  .build();

    final ScanQuery normalizedQuery = normalizer.normalize(query);

    Assert.assertEquals(ImmutableList.of("cnt", "dim1"), normalizedQuery.getColumns());
    Assert.assertEquals(ImmutableList.of(ColumnType.LONG, ColumnType.STRING), normalizedQuery.getColumnTypes());
    Assert.assertNull(normalizedQuery.context().get(DruidQuery.CTX_SCAN_SIGNATURE));
  }

  @Test
  public void testExistingScanSignatureIsRespected() throws Exception
  {
    final NativeScanQueryNormalizer normalizer = createNormalizer(dataSourceSignature());
    final RowSignature providedSignature = RowSignature.builder()
                                                       .add("cnt", ColumnType.LONG)
                                                       .add("dim1", ColumnType.STRING)
                                                       .build();
    final String providedSignatureString = JSON_MAPPER.writeValueAsString(providedSignature);

    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource("foo")
                                  .intervals(intervals("2000/2001"))
                                  .columns("cnt", "dim1")
                                  .context(ImmutableMap.of(
                                      DruidQuery.CTX_SCAN_SIGNATURE,
                                      providedSignatureString
                                  ))
                                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                  .build();

    final ScanQuery normalizedQuery = normalizer.normalize(query);

    Assert.assertSame(query, normalizedQuery);
    Assert.assertNull(normalizedQuery.getColumnTypes());
    Assert.assertEquals(providedSignatureString, normalizedQuery.context().getString(DruidQuery.CTX_SCAN_SIGNATURE));
  }

  @Test
  public void testExistingColumnTypesAreKept() throws Exception
  {
    final NativeScanQueryNormalizer normalizer = createNormalizer(dataSourceSignature());

    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource("foo")
                                  .intervals(intervals("2000/2001"))
                                  .columns("cnt", "dim1")
                                  .columnTypes(ColumnType.LONG, ColumnType.STRING)
                                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                  .build();

    final ScanQuery normalizedQuery = normalizer.normalize(query);

    Assert.assertSame(query, normalizedQuery);
  }

  @Test
  public void testJoinDataSourceUsesCentralizedSchemaForChildren() throws Exception
  {
    final NativeScanQueryNormalizer normalizer = createNormalizer(ImmutableMap.of(
        "foo",
        dataSourceSignature(),
        "bar",
        RowSignature.builder()
                    .add("dim2", ColumnType.STRING)
                    .add("m1", ColumnType.FLOAT)
                    .build()
    ));

    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource(JoinDataSource.create(
                                      TableDataSource.create("foo"),
                                      TableDataSource.create("bar"),
                                      "j.",
                                      JoinConditionAnalysis.forExpression(
                                          "dim1 == \"j.dim2\"",
                                          "j.",
                                          ExprMacroTable.nil()
                                      ),
                                      JoinType.LEFT,
                                      null,
                                      null
                                  ))
                                  .intervals(intervals("2000/2001"))
                                  .columns("dim1", "j.m1")
                                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                  .build();

    final ScanQuery normalizedQuery = normalizer.normalize(query);

    Assert.assertEquals(ImmutableList.of("dim1", "j.m1"), normalizedQuery.getColumns());
    Assert.assertEquals(ImmutableList.of(ColumnType.STRING, ColumnType.FLOAT), normalizedQuery.getColumnTypes());
    Assert.assertNull(normalizedQuery.context().get(DruidQuery.CTX_SCAN_SIGNATURE));
  }

  @Test
  public void testQueryDataSourceUsesSubquerySignature() throws Exception
  {
    final NativeScanQueryNormalizer normalizer = createNormalizer(dataSourceSignature());
    final ScanQuery innerQuery = Druids.newScanQueryBuilder()
                                       .dataSource("foo")
                                       .intervals(intervals("2000/2001"))
                                       .columns("cnt", "dim1")
                                       .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                       .build();

    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource(new QueryDataSource(innerQuery))
                                  .intervals(intervals("2000/2001"))
                                  .columns("dim1")
                                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                  .build();

    final ScanQuery normalizedQuery = normalizer.normalize(query);

    Assert.assertEquals(ImmutableList.of("dim1"), normalizedQuery.getColumns());
    Assert.assertEquals(ImmutableList.of(ColumnType.STRING), normalizedQuery.getColumnTypes());
    Assert.assertNull(normalizedQuery.context().get(DruidQuery.CTX_SCAN_SIGNATURE));

    final QueryDataSource normalizedDataSource = (QueryDataSource) normalizedQuery.getDataSource();
    final ScanQuery normalizedInnerQuery = (ScanQuery) normalizedDataSource.getQuery();
    Assert.assertEquals(ImmutableList.of("cnt", "dim1"), normalizedInnerQuery.getColumns());
    Assert.assertEquals(ImmutableList.of(ColumnType.LONG, ColumnType.STRING), normalizedInnerQuery.getColumnTypes());
    Assert.assertNull(normalizedInnerQuery.context().get(DruidQuery.CTX_SCAN_SIGNATURE));
  }

  @Test
  public void testQueryDataSourceUsesSubqueryScanSignatureFallback() throws Exception
  {
    final NativeScanQueryNormalizer normalizer = createNormalizer(dataSourceSignature());
    final RowSignature innerSignature = RowSignature.builder()
                                                    .add("dim1", ColumnType.STRING)
                                                    .build();
    final ScanQuery innerQuery = Druids.newScanQueryBuilder()
                                       .dataSource("foo")
                                       .intervals(intervals("2000/2001"))
                                       .columns("dim1")
                                       .context(ImmutableMap.of(
                                           DruidQuery.CTX_SCAN_SIGNATURE,
                                           JSON_MAPPER.writeValueAsString(innerSignature)
                                       ))
                                       .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                       .build();

    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource(new QueryDataSource(innerQuery))
                                  .intervals(intervals("2000/2001"))
                                  .columns("dim1")
                                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                  .build();

    final ScanQuery normalizedQuery = normalizer.normalize(query);

    Assert.assertEquals(ImmutableList.of("dim1"), normalizedQuery.getColumns());
    Assert.assertEquals(ImmutableList.of(ColumnType.STRING), normalizedQuery.getColumnTypes());
  }

  @Test
  public void testQueryDataSourceUsesSubqueryScanSignatureWhenBothSignatureFieldsExist() throws Exception
  {
    final NativeScanQueryNormalizer normalizer = createNormalizer(dataSourceSignature());
    final RowSignature deprecatedInnerSignature = RowSignature.builder()
                                                             .add("dim1", ColumnType.STRING)
                                                             .build();
    final ScanQuery innerQuery = Druids.newScanQueryBuilder()
                                       .dataSource("foo")
                                       .intervals(intervals("2000/2001"))
                                       .columns("dim1")
                                       .columnTypes(ColumnType.LONG)
                                       .context(ImmutableMap.of(
                                           DruidQuery.CTX_SCAN_SIGNATURE,
                                           JSON_MAPPER.writeValueAsString(deprecatedInnerSignature)
                                       ))
                                       .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                       .build();

    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource(new QueryDataSource(innerQuery))
                                  .intervals(intervals("2000/2001"))
                                  .columns("dim1")
                                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                  .build();

    final ScanQuery normalizedQuery = normalizer.normalize(query);

    Assert.assertEquals(ImmutableList.of("dim1"), normalizedQuery.getColumns());
    Assert.assertEquals(ImmutableList.of(ColumnType.STRING), normalizedQuery.getColumnTypes());
  }

  @Test
  public void testJoinDataSourceNormalizesQueryDataSourceChildren() throws Exception
  {
    final NativeScanQueryNormalizer normalizer = createNormalizer(ImmutableMap.of(
        "foo",
        dataSourceSignature(),
        "bar",
        RowSignature.builder()
                    .add("dim2", ColumnType.STRING)
                    .add("m1", ColumnType.FLOAT)
                    .build()
    ));
    final ScanQuery innerQuery = Druids.newScanQueryBuilder()
                                       .dataSource("foo")
                                       .intervals(intervals("2000/2001"))
                                       .columns("cnt", "dim1")
                                       .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                       .build();

    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource(JoinDataSource.create(
                                      new QueryDataSource(innerQuery),
                                      TableDataSource.create("bar"),
                                      "j.",
                                      JoinConditionAnalysis.forExpression(
                                          "dim1 == \"j.dim2\"",
                                          "j.",
                                          ExprMacroTable.nil()
                                      ),
                                      JoinType.LEFT,
                                      null,
                                      null
                                  ))
                                  .intervals(intervals("2000/2001"))
                                  .columns("dim1", "j.m1")
                                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                  .build();

    final ScanQuery normalizedQuery = normalizer.normalize(query);

    Assert.assertEquals(ImmutableList.of("dim1", "j.m1"), normalizedQuery.getColumns());
    Assert.assertEquals(ImmutableList.of(ColumnType.STRING, ColumnType.FLOAT), normalizedQuery.getColumnTypes());
    Assert.assertNull(normalizedQuery.context().get(DruidQuery.CTX_SCAN_SIGNATURE));

    final JoinDataSource normalizedDataSource = (JoinDataSource) normalizedQuery.getDataSource();
    final QueryDataSource normalizedLeft = (QueryDataSource) normalizedDataSource.getLeft();
    final ScanQuery normalizedInnerQuery = (ScanQuery) normalizedLeft.getQuery();
    Assert.assertEquals(ImmutableList.of("cnt", "dim1"), normalizedInnerQuery.getColumns());
    Assert.assertEquals(ImmutableList.of(ColumnType.LONG, ColumnType.STRING), normalizedInnerQuery.getColumnTypes());
    Assert.assertNull(normalizedInnerQuery.context().get(DruidQuery.CTX_SCAN_SIGNATURE));
  }

  private static NativeScanQueryNormalizer createNormalizer(final RowSignature dataSourceSignature)
  {
    return createNormalizer(ImmutableMap.of("foo", dataSourceSignature));
  }

  private static NativeScanQueryNormalizer createNormalizer(final Map<String, RowSignature> dataSourceSignatures)
  {
    return new NativeScanQueryNormalizer(JSON_MAPPER, createCoordinatorClient(dataSourceSignatures));
  }

  private static MultipleIntervalSegmentSpec intervals(final String interval)
  {
    return new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of(interval)));
  }

  private static RowSignature dataSourceSignature()
  {
    return RowSignature.builder()
                       .add("__time", ColumnType.LONG)
                       .add("cnt", ColumnType.LONG)
                       .add("dim1", ColumnType.STRING)
                       .build();
  }

  private static CoordinatorClient createCoordinatorClient(final Map<String, RowSignature> dataSourceSignatures)
  {
    final CoordinatorClient coordinatorClient = Mockito.mock(CoordinatorClient.class);
    Mockito.when(coordinatorClient.fetchDataSourceInformation(ArgumentMatchers.anySet())).thenAnswer(invocation -> {
      final Set<String> dataSourceNames = invocation.getArgument(0);
      final List<DataSourceInformation> response = new ArrayList<>();

      for (final String dataSourceName : dataSourceNames) {
        final RowSignature rowSignature = dataSourceSignatures.get(dataSourceName);
        if (rowSignature != null) {
          response.add(new DataSourceInformation(dataSourceName, rowSignature));
        }
      }

      return Futures.immediateFuture(response);
    });
    return coordinatorClient;
  }
}
