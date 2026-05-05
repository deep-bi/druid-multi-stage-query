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
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.query.Druids;
import org.apache.druid.query.MapQueryToolChestWarehouse;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.metadata.SegmentMetadataQueryConfig;
import org.apache.druid.query.metadata.SegmentMetadataQueryQueryToolChest;
import org.apache.druid.query.metadata.metadata.AllColumnIncluderator;
import org.apache.druid.query.metadata.metadata.ColumnAnalysis;
import org.apache.druid.query.metadata.metadata.ListColumnIncluderator;
import org.apache.druid.query.metadata.metadata.SegmentAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.log.TestRequestLogger;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

public class NativeScanQueryNormalizerTest
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();
  private static final AuthenticationResult AUTHENTICATION_RESULT =
      new AuthenticationResult("allowAll", "allowAll", null, null);

  @BeforeClass
  public static void setupClass()
  {
    ExpressionProcessing.initializeForTests();
    NullHandling.initializeForTests();
  }

  @Test
  public void testEmptyColumnsUseAllSegmentColumnsAndVirtualColumns() throws Exception
  {
    final NativeScanQueryNormalizer normalizer = new NativeScanQueryNormalizer(
        JSON_MAPPER,
        createLifecycleFactory((query, intervals) -> Sequences.simple(Collections.singletonList(segmentAnalysis()))),
        AUTHENTICATION_RESULT
    );

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

    final Query<?> queryWithSignature = normalizer.normalize(query);

    Assert.assertEquals(
        RowSignature.builder()
                    .add("__time", ColumnType.LONG)
                    .add("cnt", ColumnType.LONG)
                    .add("dim1", ColumnType.STRING)
                    .add("v0", ColumnType.STRING)
                    .build(),
        readScanSignature(queryWithSignature)
    );
    final List<String> columns = ((ScanQuery) queryWithSignature).getColumns();
    Assert.assertEquals(ImmutableList.of("__time", "cnt", "dim1", "v0"), columns);
  }

  @Test
  public void testFallsBackToEternityMetadataWhenIntervalHasNoSegments() throws Exception
  {
    final List<SegmentMetadataQuery> metadataQueries = new ArrayList<>();
    final NativeScanQueryNormalizer normalizer = new NativeScanQueryNormalizer(
        JSON_MAPPER,
        createLifecycleFactory(
            (query, intervals) -> {
              metadataQueries.add((SegmentMetadataQuery) query);

              if (Intervals.ONLY_ETERNITY.equals(intervals)) {
                return Sequences.simple(Collections.singletonList(segmentAnalysis()));
              } else {
                return Sequences.empty();
              }
            }
        ),
        AUTHENTICATION_RESULT
    );

    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource("foo")
                                  .intervals(intervals("2000/2001"))
                                  .columns("cnt", "dim1")
                                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                  .build();

    final Query<?> queryWithSignature = normalizer.normalize(query);

    Assert.assertEquals(
        RowSignature.builder()
                    .add("cnt", ColumnType.LONG)
                    .add("dim1", ColumnType.STRING)
                    .build(),
        readScanSignature(queryWithSignature)
    );
    Assert.assertEquals(ImmutableList.of("cnt", "dim1"), ((ScanQuery) queryWithSignature).getColumns());

    Assert.assertEquals(2, metadataQueries.size());
    Assert.assertEquals(ImmutableList.of(Intervals.of("2000/2001")), metadataQueries.get(0).getIntervals());
    Assert.assertEquals(Intervals.ONLY_ETERNITY, metadataQueries.get(1).getIntervals());
    Assert.assertTrue(metadataQueries.get(0).getToInclude() instanceof ListColumnIncluderator);
    Assert.assertTrue(metadataQueries.get(1).getToInclude() instanceof ListColumnIncluderator);
  }

  @Test
  public void testExistingScanSignatureIsNotRegenerated() throws Exception
  {
    final NativeScanQueryNormalizer normalizer = new NativeScanQueryNormalizer(
        JSON_MAPPER,
        createLifecycleFactory(
            (query, intervals) -> {
              Assert.fail("scanSignature should not be generated when it is already present");
              return Sequences.empty();
            }
        ),
        AUTHENTICATION_RESULT
    );
    final RowSignature providedSignature = RowSignature.builder()
                                                       .add("cnt", ColumnType.LONG)
                                                       .add("dim1", ColumnType.STRING)
                                                       .build();

    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource("foo")
                                  .intervals(intervals("2000/2001"))
                                  .columns("cnt", "dim1")
                                  .context(ImmutableMap.of(
                                      DruidQuery.CTX_SCAN_SIGNATURE,
                                      JSON_MAPPER.writeValueAsString(providedSignature)
                                  ))
                                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                  .build();

    final Query<?> normalizedQuery = normalizer.normalize(query);

    Assert.assertEquals(providedSignature, readScanSignature(normalizedQuery));
    Assert.assertEquals(ImmutableList.of("cnt", "dim1"), ((ScanQuery) normalizedQuery).getColumns());
  }

  @Test
  public void testEmptyColumnsMetadataQueryIncludesAllColumns() throws Exception
  {
    final List<SegmentMetadataQuery> metadataQueries = new ArrayList<>();
    final NativeScanQueryNormalizer normalizer = new NativeScanQueryNormalizer(
        JSON_MAPPER,
        createLifecycleFactory(
            (query, intervals) -> {
              metadataQueries.add((SegmentMetadataQuery) query);
              return Sequences.simple(Collections.singletonList(segmentAnalysis()));
            }
        ),
        AUTHENTICATION_RESULT
    );

    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource("foo")
                                  .intervals(intervals("2000/2001"))
                                  .columns(Collections.emptyList())
                                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                  .build();

    normalizer.normalize(query);

    Assert.assertEquals(2, metadataQueries.size());
    Assert.assertTrue(metadataQueries.get(0).getToInclude() instanceof AllColumnIncluderator);
    Assert.assertTrue(metadataQueries.get(1).getToInclude() instanceof ListColumnIncluderator);
  }

  private static RowSignature readScanSignature(final Query<?> query) throws Exception
  {
    return JSON_MAPPER.readValue(query.context().getString(DruidQuery.CTX_SCAN_SIGNATURE), RowSignature.class);
  }

  private static MultipleIntervalSegmentSpec intervals(final String interval)
  {
    return new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of(interval)));
  }

  private static SegmentAnalysis segmentAnalysis()
  {
    final LinkedHashMap<String, ColumnAnalysis> columns = new LinkedHashMap<>();
    columns.put("__time", ColumnAnalysis.builder().withType(ColumnType.LONG).withSize(0).build());
    columns.put("cnt", ColumnAnalysis.builder().withType(ColumnType.LONG).withSize(0).build());
    columns.put("dim1", ColumnAnalysis.builder().withType(ColumnType.STRING).withSize(0).build());

    return new SegmentAnalysis(
        "foo",
        null,
        columns,
        0,
        0,
        null,
        null,
        null,
        null
    );
  }

  private static QueryLifecycleFactory createLifecycleFactory(final SegmentMetadataQueryRunner runner)
  {
    return new QueryLifecycleFactory(
        warehouse(),
        new QuerySegmentWalker()
        {
          @Override
          public <T> QueryRunner<T> getQueryRunnerForIntervals(
              final Query<T> query,
              final Iterable<Interval> intervals
          )
          {
            return (queryPlus, responseContext) -> (Sequence<T>) runner.run(query, query.getIntervals());
          }

          @Override
          public <T> QueryRunner<T> getQueryRunnerForSegments(
              final Query<T> query,
              final Iterable<SegmentDescriptor> specs
          )
          {
            return getQueryRunnerForIntervals(query, query.getIntervals());
          }
        },
        new DefaultGenericQueryMetricsFactory(),
        new NoopServiceEmitter(),
        new TestRequestLogger(),
        new AuthConfig(),
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        Suppliers.ofInstance(new DefaultQueryConfig(ImmutableMap.of()))
    );
  }

  private static QueryToolChestWarehouse warehouse()
  {
    return new MapQueryToolChestWarehouse(
        ImmutableMap.of(
            SegmentMetadataQuery.class,
            new SegmentMetadataQueryQueryToolChest(new SegmentMetadataQueryConfig())
        )
    );
  }

  private interface SegmentMetadataQueryRunner
  {
    Sequence<SegmentAnalysis> run(Query<?> query, List<Interval> intervals);
  }
}
