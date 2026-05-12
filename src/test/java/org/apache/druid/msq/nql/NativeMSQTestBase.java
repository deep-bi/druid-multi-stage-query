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

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.msq.test.MSQTestBase;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.query.MapQueryToolChestWarehouse;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryQueryToolChest;
import org.apache.druid.query.metadata.SegmentMetadataQueryConfig;
import org.apache.druid.query.metadata.SegmentMetadataQueryQueryToolChest;
import org.apache.druid.query.metadata.metadata.ColumnAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryQueryToolChest;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.metadata.DataSourceInformation;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.log.TestRequestLogger;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthTestUtils;
import org.joda.time.Interval;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class NativeMSQTestBase extends MSQTestBase
{
  protected static final QueryToolChestWarehouse WAREHOUSE = new MapQueryToolChestWarehouse(ImmutableMap.<Class<? extends Query>, QueryToolChest>builder()
                                                                                                        .put(
                                                                                                            ScanQuery.class,
                                                                                                            new ScanQueryQueryToolChest(
                                                                                                                DefaultGenericQueryMetricsFactory.instance()
                                                                                                            )
                                                                                                        )
                                                                                                        .put(
                                                                                                            GroupByQuery.class,
                                                                                                            new GroupByQueryQueryToolChest(
                                                                                                                null,
                                                                                                                null
                                                                                                            )
                                                                                                        )
                                                                                                        // Added timeseries toolchest to test unsupported query exception
                                                                                                        .put(
                                                                                                            TimeseriesQuery.class,
                                                                                                            new TimeseriesQueryQueryToolChest(
                                                                                                            )
                                                                                                        )
                                                                                                        .put(
                                                                                                            SegmentMetadataQuery.class,
                                                                                                            new SegmentMetadataQueryQueryToolChest(
                                                                                                                new SegmentMetadataQueryConfig()
                                                                                                            )
                                                                                                        )
                                                                                                        .build());
  protected static final QuerySegmentWalker TEST_SEGMENT_WALKER = new QuerySegmentWalker()
  {
    @Override
    public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
    {
      if (query instanceof SegmentMetadataQuery) {
        return (queryPlus, responseContext) -> (Sequence<T>) Sequences.simple(
            Collections.singletonList(segmentAnalysis())
        );
      }
      return (queryPlus, responseContext) -> Sequences.empty();
    }

    @Override
    public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
    {
      return getQueryRunnerForIntervals(null, null);
    }
  };

  private static SegmentAnalysis segmentAnalysis()
  {
    final LinkedHashMap<String, ColumnAnalysis> columns = new LinkedHashMap<>();
    columns.put("cnt", ColumnAnalysis.builder().withType(ColumnType.LONG).withSize(0).build());
    columns.put("dim1", ColumnAnalysis.builder().withType(ColumnType.STRING).withSize(0).build());

    return new SegmentAnalysis(
        "test",
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

  protected QueryLifecycleFactory createLifecycleFactory()
  {
    return new QueryLifecycleFactory(
        WAREHOUSE,
        TEST_SEGMENT_WALKER,
        new DefaultGenericQueryMetricsFactory(),
        new NoopServiceEmitter(),
        new TestRequestLogger(),
        new AuthConfig(),
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        Suppliers.ofInstance(new DefaultQueryConfig(ImmutableMap.of()))
    );
  }

  protected CoordinatorClient createCoordinatorClient()
  {
    final RowSignature dataSourceSignature = RowSignature.builder()
                                                         .add("cnt", ColumnType.LONG)
                                                         .add("dim1", ColumnType.STRING)
                                                         .build();
    return createCoordinatorClient(ImmutableMap.of("foo", dataSourceSignature));
  }

  protected CoordinatorClient createCoordinatorClient(final Map<String, RowSignature> dataSourceSignatures)
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
