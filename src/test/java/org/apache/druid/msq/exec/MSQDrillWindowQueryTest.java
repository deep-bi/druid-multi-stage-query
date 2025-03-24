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
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.commons.lang3.StringUtils;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.msq.exec.MSQDrillWindowQueryTest.DrillWindowQueryMSQComponentSupplier;
import org.apache.druid.msq.sql.MSQTaskSqlEngine;
import org.apache.druid.msq.test.CalciteMSQTestsHelper;
import org.apache.druid.msq.test.ExtractResultsFactory;
import org.apache.druid.msq.test.MSQSQLTestOverlordServiceClient;
import org.apache.druid.msq.test.VerifyMSQSupportedNativeQueriesPredicate;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.groupby.TestGroupByBuffers;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.sql.calcite.DrillWindowQueryTest;
import org.apache.druid.sql.calcite.QueryTestBuilder;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig;
import org.apache.druid.sql.calcite.TempDirProducer;
import org.apache.druid.sql.calcite.planner.PlannerCaptureHook;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

@SqlTestFrameworkConfig.ComponentSupplier(DrillWindowQueryMSQComponentSupplier.class)
public class MSQDrillWindowQueryTest extends DrillWindowQueryTest
{
  private final Map<String, Object> queryContext = new HashMap<>(ImmutableMap.of(
      PlannerCaptureHook.NEED_CAPTURE_HOOK, true,
      QueryContexts.ENABLE_DEBUG, true,
      MultiStageQueryContext.CTX_MAX_NUM_TASKS, 5
  ));

  public static class DrillWindowQueryMSQComponentSupplier extends DrillComponentSupplier
  {
    public DrillWindowQueryMSQComponentSupplier(TempDirProducer tempFolderProducer)
    {
      super(tempFolderProducer);
    }

    @Override
    public void configureGuice(DruidInjectorBuilder builder)
    {
      super.configureGuice(builder);
      builder.addModules(CalciteMSQTestsHelper.fetchModules(tempDirProducer::newTempFolder, TestGroupByBuffers.createDefault()).toArray(new Module[0]));
      builder.addModule(new TestMSQSqlModule());
    }

    @Override
    public SqlEngine createEngine(
        QueryLifecycleFactory qlf,
        ObjectMapper queryJsonMapper,
        Injector injector
    )
    {
      return injector.getInstance(MSQTaskSqlEngine.class);
    }
  }

  @Override
  protected QueryTestBuilder testBuilder()
  {
    return new QueryTestBuilder(new CalciteTestConfig(true))
        .addCustomRunner(new ExtractResultsFactory(() -> (MSQSQLTestOverlordServiceClient) ((MSQTaskSqlEngine) queryFramework().engine()).overlordClient()))
        .skipVectorize(true)
        .verifyNativeQueries(new VerifyMSQSupportedNativeQueriesPredicate());
  }

  @Override
  protected Map<String, Object> getQueryContext()
  {
    return queryContext;
  }

  @Override
  @DrillTest("druid_queries/empty_over_clause/multiple_empty_over_1")
  @Test
  public void test_empty_over_multiple_empty_over_1()
  {
    useSingleWorker();
    windowQueryTest();
  }

  @Override
  @DrillTest("druid_queries/empty_over_clause/single_empty_over_1")
  @Test
  public void test_empty_over_single_empty_over_1()
  {
    useSingleWorker();
    windowQueryTest();
  }

  @Override
  @DrillTest("druid_queries/empty_over_clause/single_empty_over_2")
  @Test
  public void test_empty_over_single_empty_over_2()
  {
    useSingleWorker();
    windowQueryTest();
  }

  @Override
  @DrillTest("druid_queries/empty_and_non_empty_over/wikipedia_query_1")
  @Test
  public void test_empty_and_non_empty_over_wikipedia_query_1()
  {
    useSingleWorker();
    windowQueryTest();
  }

  @Override
  @DrillTest("druid_queries/empty_and_non_empty_over/wikipedia_query_2")
  @Test
  public void test_empty_and_non_empty_over_wikipedia_query_2()
  {
    useSingleWorker();
    windowQueryTest();
  }

  @Override
  @DrillTest("druid_queries/empty_and_non_empty_over/wikipedia_query_3")
  @Test
  public void test_empty_and_non_empty_over_wikipedia_query_3()
  {
    useSingleWorker();
    windowQueryTest();
  }

  @Override
  @DrillTest("druid_queries/over_clause_only_partitioning/multiple_over_multiple_partition_columns_2")
  @Test
  public void test_over_clause_with_only_partitioning_multiple_over_multiple_partition_columns_2()
  {
    useSingleWorker();
    windowQueryTest();
  }

  @Override
  @DrillTest("druid_queries/over_clause_only_partitioning/multiple_over_different_partition_column")
  @Test
  public void test_over_clause_with_only_partitioning_multiple_over_different_partition_column()
  {
    useSingleWorker();
    windowQueryTest();
  }

  @Override
  @DrillTest("ntile_func/ntileFn_47")
  @Test
  public void test_ntile_func_ntileFn_47()
  {
    useSingleWorker();
    windowQueryTest();
  }

  @Override
  @DrillTest("ntile_func/ntileFn_49")
  @Test
  public void test_ntile_func_ntileFn_49()
  {
    useSingleWorker();
    windowQueryTest();
  }

  @Override
  @DrillTest("ntile_func/ntileFn_50")
  @Test
  public void test_ntile_func_ntileFn_50()
  {
    useSingleWorker();
    windowQueryTest();
  }

  @Override
  @DrillTest("ntile_func/ntileFn_51")
  @Test
  public void test_ntile_func_ntileFn_51()
  {
    useSingleWorker();
    windowQueryTest();
  }

  @Override
  @DrillTest("ntile_func/ntileFn_52")
  @Test
  public void test_ntile_func_ntileFn_52()
  {
    useSingleWorker();
    windowQueryTest();
  }

  @Override
  @DrillTest("ntile_func/ntileFn_53")
  @Test
  public void test_ntile_func_ntileFn_53()
  {
    useSingleWorker();
    windowQueryTest();
  }

  @Override
  @DrillTest("ntile_func/ntileFn_54")
  @Test
  public void test_ntile_func_ntileFn_54()
  {
    useSingleWorker();
    windowQueryTest();
  }

  @Override
  @DrillTest("ntile_func/ntileFn_55")
  @Test
  public void test_ntile_func_ntileFn_55()
  {
    useSingleWorker();
    windowQueryTest();
  }

  @Override
  @DrillTest("ntile_func/ntileFn_56")
  @Test
  public void test_ntile_func_ntileFn_56()
  {
    useSingleWorker();
    windowQueryTest();
  }

  @Override
  @DrillTest("ntile_func/ntileFn_57")
  @Test
  public void test_ntile_func_ntileFn_57()
  {
    useSingleWorker();
    windowQueryTest();
  }

  @Override
  @DrillTest("ntile_func/ntileFn_58")
  @Test
  public void test_ntile_func_ntileFn_58()
  {
    useSingleWorker();
    windowQueryTest();
  }

  // Overrides the parent test to access the jar content properly
  @Override
  @Test
  public void ensureAllDeclared() throws Exception
  {
    URL windowQueriesUrl = ClassLoader.getSystemResource("drill/window/queries/");
    URI uri = windowQueriesUrl.toURI();
    Path windowFolder;
    if ("file".equals(uri.getScheme())) {
      windowFolder = Paths.get(uri);
    } else if ("jar".equals(uri.getScheme())) {
      String[] jarParts = uri.toString().split("!");
      Path jarPath = Paths.get(new URI(StringUtils.replace(jarParts[0], "jar:", "")));

      File tempDir = FileUtils.createTempDir();
      try (JarFile jar = new JarFile(jarPath.toFile())) {
        String resourcePath = "drill/window/queries/";
        for (JarEntry entry : Collections.list(jar.entries())) {
          if (entry.getName().startsWith(resourcePath) && entry.getName().endsWith(".q")) {
            File file = new File(tempDir, entry.getName().substring(resourcePath.length()));
            FileUtils.mkdirp(file.getParentFile());
            try (InputStream in = jar.getInputStream(entry)) {
              Files.copy(in, file.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }
          }
        }
      }
      windowFolder = tempDir.toPath();
    } else {
      throw new IllegalArgumentException("Unsupported URI scheme: " + uri);
    }

    Set<String> allCases = org.apache.commons.io.FileUtils.streamFiles(windowFolder.toFile(), true, new String[]{"q"})
                                                          .map((file) -> windowFolder.relativize(file.toPath()).toString())
                                                          .sorted()
                                                          .collect(
                                        Collectors.toCollection(LinkedHashSet::new));
    Method[] var4 = DrillWindowQueryTest.class.getDeclaredMethods();

    for (Method method : var4) {
      DrillTest ann = method.getAnnotation(DrillTest.class);
      if (method.getAnnotation(Test.class) != null && ann != null && !allCases.remove(ann.value() + ".q")) {
        Assertions.fail(String.format(
            Locale.ENGLISH,
            "Testcase [%s] references invalid file [%s].",
            method.getName(),
            ann.value()
        ));
      }
    }

    for (String string : allCases) {
      string = string.substring(0, string.lastIndexOf(46));
      System.out.printf(
          Locale.ENGLISH,
          "@%s( \"%s\" )\n@Test\npublic void test_%s() {\n    windowQueryTest();\n}\n",
          DrillTest.class.getSimpleName(),
          string,
          string.replace('/', '_')
      );
    }

    Assertions.assertEquals(
        0L,
        allCases.size(),
        "Found some non-declared testcases; please add the new testcases printed to the console!"
    );
  }

  /*
  Queries having window functions can give multiple correct results because of using MixShuffleSpec in the previous stage.
  So we want to use a single worker to get the same result everytime for such test cases.
   */
  private void useSingleWorker()
  {
    queryContext.put(MultiStageQueryContext.CTX_MAX_NUM_TASKS, 2);
  }
}
