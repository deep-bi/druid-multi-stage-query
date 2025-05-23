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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.sql.MSQTaskSqlEngine;
import org.apache.druid.sql.calcite.CalciteQueryTest;
import org.apache.druid.sql.calcite.QueryTestBuilder;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig;
import org.junit.Assert;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

/**
 * Runs {@link CalciteQueryTest} but with MSQ engine
 */
@SqlTestFrameworkConfig.ComponentSupplier(StandardMSQComponentSupplier.class)
public class CalciteSelectQueryMSQTest extends CalciteQueryTest
{
  @Override
  protected QueryTestBuilder testBuilder()
  {
    return new QueryTestBuilder(new CalciteTestConfig(true))
        .addCustomRunner(new ExtractResultsFactory(() -> (MSQSQLTestOverlordServiceClient) ((MSQTaskSqlEngine) queryFramework().engine()).overlordClient()))
        .skipVectorize(true)
        .verifyNativeQueries(new VerifyMSQSupportedNativeQueriesPredicate());
  }

  @Disabled
  @Override
  @Test
  public void testCannotInsertWithNativeEngine()
  {

  }

  @Disabled
  @Override
  @Test
  public void testCannotReplaceWithNativeEngine()
  {

  }

  @Disabled
  @Override
  @Test
  public void testRequireTimeConditionSimpleQueryNegative()
  {

  }

  @Disabled
  @Override
  @Test
  public void testRequireTimeConditionSubQueryNegative()
  {

  }

  @Disabled
  @Override
  @Test
  public void testRequireTimeConditionSemiJoinNegative()
  {

  }

  @Disabled
  @Override
  @Test
  public void testExactCountDistinctWithFilter()
  {

  }

  @Disabled
  @Override
  @Test
  public void testExactCountDistinctWithFilter2()
  {

  }

  @Disabled
  @Override
  @Test
  public void testUnplannableScanOrderByNonTime()
  {

  }

  @Disabled
  @Override
  @Test
  public void testUnSupportedNullsFirst()
  {
  }

  @Disabled
  @Override
  @Test
  public void testUnSupportedNullsLast()
  {
  }

  /**
   * Same query as {@link CalciteQueryTest#testArrayAggQueryOnComplexDatatypes}. ARRAY_AGG is not supported in MSQ currently.
   * Once support is added, this test can be removed and msqCompatible() can be added to the one in CalciteQueryTest.
   */
  @Test
  @Override
  public void testArrayAggQueryOnComplexDatatypes()
  {
    try {
      testQuery("SELECT ARRAY_AGG(unique_dim1) FROM druid.foo", ImmutableList.of(), ImmutableList.of());
      Assert.fail("query execution should fail");
    }
    catch (ISE e) {
      Assert.assertTrue(
          e.getMessage().contains("Cannot handle column [a0] with type [ARRAY<COMPLEX<hyperUnique>>]")
      );
    }
  }

  @Test
  @Timeout(value = 40000, unit = TimeUnit.MILLISECONDS)
  public void testJoinMultipleTablesWithWhereCondition()
  {
    testBuilder()
        .queryContext(
            ImmutableMap.of(
                "sqlJoinAlgorithm", "sortMerge"
            )
        )
        .sql(
            "SELECT f2.dim3,sum(f6.m1 * (1- f6.m2)) FROM"
                + " druid.foo as f5, "
                + " druid.foo as f6,  "
                + " druid.numfoo as f7, "
                + " druid.foo2 as f2, "
                + " druid.numfoo as f3, "
                + " druid.foo as f4, "
                + " druid.numfoo as f1, "
                + " druid.foo2 as f8  "
                + "where true"
                + " and f1.dim1 = f2.dim2 "
                + " and f3.dim1 = f4.dim2 "
                + " and f5.dim1 = f6.dim2 "
                + " and f7.dim2 = f8.dim3 "
                + " and f2.dim1 = f4.dim2 "
                + " and f6.dim1 = f8.dim2 "
                + " and f1.dim1 = f7.dim2 "
                + " and f8.dim2 = 'x' "
                + " and f3.__time >= date '2011-11-11' "
                + " and f3.__time < date '2013-11-11' "
                + "group by 1 "
                + "order by 2 desc limit 1001"
        )
        .run();
  }

  @Override
  @Test
  public void testFilterParseLongNullable()
  {
    // this isn't really correct in default value mode, the result should be ImmutableList.of(new Object[]{0L})
    // but MSQ is missing default aggregator values in empty group results. this override can be removed when this
    // is fixed
    testBuilder().queryContext(QUERY_CONTEXT_DEFAULT)
                 .sql("select count(*) from druid.foo where parse_long(dim1, 10) is null")
                 .expectedResults(
                     ImmutableList.of(new Object[]{4L})
                 )
                 .run();
  }
}
