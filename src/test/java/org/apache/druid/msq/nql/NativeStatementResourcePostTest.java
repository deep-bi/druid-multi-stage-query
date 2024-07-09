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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.msq.indexing.MSQControllerTask;
import org.apache.druid.msq.nql.resources.NativeStatementResource;
import org.apache.druid.msq.sql.StatementState;
import org.apache.druid.msq.sql.entity.PageInformation;
import org.apache.druid.msq.sql.entity.ResultSetInformation;
import org.apache.druid.msq.sql.resources.SqlStatementResourceTest;
import org.apache.druid.msq.test.MSQTestFileUtils;
import org.apache.druid.msq.test.MSQTestOverlordServiceClient;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.server.mocks.MockHttpServletRequest;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.storage.NilStorageConnector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.apache.druid.msq.sql.resources.SqlMSQStatementResourcePostTest.responseToByteArray;

public class NativeStatementResourcePostTest extends NativeMSQTestBase
{

  private static final String SIMPLE_SCAN_QUERY = "{\n"
                                                  + "    \"queryType\": \"scan\",\n"
                                                  + "    \"dataSource\": \"foo\",\n"
                                                  + "    \"granularity\": \"hour\",\n"
                                                  + "    \"resultFormat\":\"compactedList\",\n"
                                                  + "    \"legacy\":false,\n"
                                                  + "    \"intervals\": [\n"
                                                  + "      \"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"\n"
                                                  + "    ],\n"
                                                  + "    \"columns\": [\n"
                                                  + "      \"cnt\",\n"
                                                  + "      \"dim1\"\n"
                                                  + "    ],\n"
                                                  + "    \"context\":\n"
                                                  + "      {\n"
                                                  + "        \"__user\": \"allowAll\",\n"
                                                  + "        \"executionMode\": \"ASYNC\",\n"
                                                  + "        \"maxNumTasks\": 2,\n"
                                                  + "        \"scanSignature\": \"[{\\\"name\\\":\\\"cnt\\\",\\\"type\\\":\\\"LONG\\\"},"
                                                  + "{\\\"name\\\":\\\"dim1\\\",\\\"type\\\":\\\"STRING\\\"}]\"\n"
                                                  + "      }\n"
                                                  + "}";
  private static final String SIMPLE_GROUP_BY_QUERY = "{\n"
                                                      + "    \"queryType\": \"groupBy\",\n"
                                                      + "    \"dataSource\": \"foo\",\n"
                                                      + "    \"granularity\": \"all\",\n"
                                                      + "    \"intervals\": [\n"
                                                      + "      \"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"\n"
                                                      + "    ],\n"
                                                      + "    \"dimensions\": [\n"
                                                      + "      {\n"
                                                      + "        \"type\": \"default\",\n"
                                                      + "        \"dimension\": \"cnt\",\n"
                                                      + "        \"outputName\": \"dim\",\n"
                                                      + "        \"outputType\": \"LONG\"\n"
                                                      + "      }\n"
                                                      + "    ],\n"
                                                      + "    \"aggregations\": [\n"
                                                      + "      {\n"
                                                      + "        \"type\": \"count\",\n"
                                                      + "        \"name\": \"count\"\n"
                                                      + "      }\n"
                                                      + "    ],\n"
                                                      + "    \"context\":\n"
                                                      + "      {\n"
                                                      + "        \"__user\": \"allowAll\",\n"
                                                      + "        \"executionMode\": \"ASYNC\",\n"
                                                      + "        \"maxNumTasks\": 2\n"
                                                      + "      }\n"
                                                      + "}";
  private static final String SIMPLE_SCAN_QUERY_WITH_DURABLE = "{\n"
                                                               + "    \"queryType\": \"scan\",\n"
                                                               + "    \"dataSource\": \"foo\",\n"
                                                               + "    \"granularity\": \"hour\",\n"
                                                               + "    \"resultFormat\":\"compactedList\",\n"
                                                               + "    \"legacy\":false,\n"
                                                               + "    \"intervals\": [\n"
                                                               + "      \"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"\n"
                                                               + "    ],\n"
                                                               + "    \"columns\": [\n"
                                                               + "      \"cnt\",\n"
                                                               + "      \"dim1\"\n"
                                                               + "    ],\n"
                                                               + "    \"context\":\n"
                                                               + "      {\n"
                                                               + "        \"__user\": \"allowAll\",\n"
                                                               + "        \"executionMode\": \"ASYNC\",\n"
                                                               + "        \"maxNumTasks\": 2,\n"
                                                               + "        \"selectDestination\": \"durableStorage\",\n"
                                                               + "        \"scanSignature\": \"[{\\\"name\\\":\\\"cnt\\\",\\\"type\\\":\\\"LONG\\\"},"
                                                               + "{\\\"name\\\":\\\"dim1\\\",\\\"type\\\":\\\"STRING\\\"}]\"\n"
                                                               + "      }\n"
                                                               + "}";
  private static final String SIMPLE_SCAN_QUERY_WITH_DURABLE_AND_RPP = "{\n"
                                                                       + "    \"queryType\": \"scan\",\n"
                                                                       + "    \"dataSource\": \"foo\",\n"
                                                                       + "    \"granularity\": \"hour\",\n"
                                                                       + "    \"resultFormat\":\"compactedList\",\n"
                                                                       + "    \"legacy\":false,\n"
                                                                       + "    \"intervals\": [\n"
                                                                       + "      \"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"\n"
                                                                       + "    ],\n"
                                                                       + "    \"columns\": [\n"
                                                                       + "      \"cnt\",\n"
                                                                       + "      \"dim1\"\n"
                                                                       + "    ],\n"
                                                                       + "    \"context\":\n"
                                                                       + "      {\n"
                                                                       + "        \"__user\": \"allowAll\",\n"
                                                                       + "        \"executionMode\": \"ASYNC\",\n"
                                                                       + "        \"maxNumTasks\": 2,\n"
                                                                       + "        \"selectDestination\": \"durableStorage\",\n"
                                                                       + "        \"rowsPerPage\": 2,\n"
                                                                       + "        \"scanSignature\": \"[{\\\"name\\\":\\\"cnt\\\",\\\"type\\\":\\\"LONG\\\"},"
                                                                       + "{\\\"name\\\":\\\"dim1\\\",\\\"type\\\":\\\"STRING\\\"}]\"\n"
                                                                       + "      }\n"
                                                                       + "}";

  private static final String MULTI_WORKER_SCAN_TEMPLATE = "{\n"
                                                           + "  \"queryType\": \"scan\",\n"
                                                           + "  \"dataSource\": {\n"
                                                           + "    \"type\": \"external\",\n"
                                                           + "    \"inputSource\": {\n"
                                                           + "      \"type\": \"local\",\n"
                                                           + "      \"files\": [\n"
                                                           + "        \"%s\",\"%s\"\n"
                                                           + "      ]\n"
                                                           + "    },\n"
                                                           + "    \"inputFormat\": {\n"
                                                           + "      \"type\": \"json\",\n"
                                                           + "      \"keepNullColumns\": false,\n"
                                                           + "      \"assumeNewlineDelimited\": false,\n"
                                                           + "      \"useJsonNodeReader\": false\n"
                                                           + "    },\n"
                                                           + "    \"signature\": [\n"
                                                           + "      {\n"
                                                           + "        \"name\": \"timestamp\",\n"
                                                           + "        \"type\": \"STRING\"\n"
                                                           + "      },\n"
                                                           + "      {\n"
                                                           + "        \"name\": \"page\",\n"
                                                           + "        \"type\": \"STRING\"\n"
                                                           + "      },\n"
                                                           + "      {\n"
                                                           + "        \"name\": \"user-id\",\n"
                                                           + "        \"type\": \"STRING\"\n"
                                                           + "      }\n"
                                                           + "    ]\n"
                                                           + "  },\n"
                                                           + "  \"intervals\": {\n"
                                                           + "    \"type\": \"intervals\",\n"
                                                           + "    \"intervals\": [\n"
                                                           + "      \"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"\n"
                                                           + "    ]\n"
                                                           + "  },\n"
                                                           + "  \"virtualColumns\": [\n"
                                                           + "    {\n"
                                                           + "      \"type\": \"expression\",\n"
                                                           + "      \"name\": \"v0\",\n"
                                                           + "      \"expression\": \"timestamp_floor(timestamp_parse(\\\"timestamp\\\",null,'UTC'),'P1D',null,'UTC')\",\n"
                                                           + "      \"outputType\": \"LONG\"\n"
                                                           + "    }\n"
                                                           + "  ],\n"
                                                           + "  \"resultFormat\": \"compactedList\",\n"
                                                           + "  \"filter\": {\n"
                                                           + "    \"type\": \"like\",\n"
                                                           + "    \"dimension\": \"user\",\n"
                                                           + "    \"pattern\": \"%%ot%%\",\n"
                                                           + "    \"escape\": null,\n"
                                                           + "    \"extractionFn\": null\n"
                                                           + "  },\n"
                                                           + "  \"columns\": [\n"
                                                           + "    \"v0\",\n"
                                                           + "    \"user\"\n"
                                                           + "  ],\n"
                                                           + "  \"legacy\": false,\n"
                                                           + "  \"context\": {\n"
                                                           + "    \"executionMode\": \"async\",\n"
                                                           + "    \"__user\": \"allowAll\",\n"
                                                           + "    \"scanSignature\": \"[{\\\"name\\\":\\\"v0\\\",\\\"type\\\":\\\"LONG\\\"},{\\\"name\\\":\\\"user\\\",\\\"type\\\":\\\"STRING\\\"}]\",\n"
                                                           + "    \"selectDestination\": \"durableStorage\",\n"
                                                           + "    \"rowsPerPage\": 2,\n"
                                                           + "    \"maxNumTasks\": 3\n"
                                                           + "  },\n"
                                                           + "  \"granularity\": {\n"
                                                           + "    \"type\": \"all\"\n"
                                                           + "  }\n"
                                                           + "}";
  private static final String DURABLE_STORAGE_UNAVAILABLE_MESSAGE =
      "The native statement api cannot read from the select destination [durableStorage] provided in "
      + "the query context [selectDestination] since it is not configured on the broker. It is recommended to "
      + "configure durable storage as it allows the user to fetch large result sets. "
      + "Please contact your cluster admin to configure durable storage.";
  private static final Map<String, ColumnType> ROW_SIGNATURE = ImmutableMap.of("count",
                                                                               ColumnType.LONG, "dim",
                                                                               ColumnType.LONG
  );
  private static final String CONTENT_TYPE_JSON = "application/json";
  private static final AuthenticationResult AUTHENTICATION_RESULT =
      new AuthenticationResult("allowAll", "allowAll", null, null);

  private NativeStatementResource resource;

  @Before
  public void init()
  {
    resource = new NativeStatementResource(
        objectMapper,
        smileMapper,
        indexingNativeServiceClient,
        createLifecycleFactory(),
        authorizerMapper,
        localFileStorageConnector
    );
  }

  @Test
  public void testNonLegacyScanQuery()
      throws JsonProcessingException // Legacy scan queries are unsupported by msq engine
  {
    List<Object[]> results = ImmutableList.of(
        new Object[]{1L, ""},
        new Object[]{
            1L,
            "10.1"
        },
        new Object[]{1L, "2"},
        new Object[]{1L, "1"},
        new Object[]{1L, "def"},
        new Object[]{1L, "abc"}
    );

    MockHttpServletRequest testServletRequest = new MockHttpServletRequest();

    testServletRequest.setAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT, AUTHENTICATION_RESULT);
    testServletRequest.contentType = CONTENT_TYPE_JSON;
    Response response = doPost(SIMPLE_SCAN_QUERY, testServletRequest);
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    String taskId = ((NativeStatementResult) response.getEntity()).getQueryId();

    NativeStatementResult expected = new NativeStatementResult(taskId, StatementState.SUCCESS,
                                                               MSQTestOverlordServiceClient.CREATED_TIME,
                                                               ImmutableMap.of(),
                                                               MSQTestOverlordServiceClient.DURATION,
                                                               new ResultSetInformation(
                                                                   6L,
                                                                   316L,
                                                                   null,
                                                                   MSQControllerTask.DUMMY_DATASOURCE_FOR_SELECT,
                                                                   results,
                                                                   ImmutableList.of(new PageInformation(0, 6L, 316L))
                                                               ),
                                                               null
    );
    Assert.assertEquals(
        objectMapper.writeValueAsString(expected),
        objectMapper.writeValueAsString(response.getEntity())
    );
  }

  @Test
  public void testGroupByQuery()
      throws JsonProcessingException
  {
    List<Object[]> results = Collections.singletonList(
        new Object[]{1L, 6L}
    );

    MockHttpServletRequest testServletRequest = new MockHttpServletRequest();

    testServletRequest.setAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT, AUTHENTICATION_RESULT);
    testServletRequest.contentType = CONTENT_TYPE_JSON;
    Response response = doPost(SIMPLE_GROUP_BY_QUERY, testServletRequest);
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    String taskId = ((NativeStatementResult) response.getEntity()).getQueryId();

    NativeStatementResult expected = new NativeStatementResult(taskId, StatementState.SUCCESS,
                                                               MSQTestOverlordServiceClient.CREATED_TIME,
                                                               ROW_SIGNATURE,
                                                               MSQTestOverlordServiceClient.DURATION,
                                                               new ResultSetInformation(
                                                                   1L,
                                                                   68L,
                                                                   null,
                                                                   MSQControllerTask.DUMMY_DATASOURCE_FOR_SELECT,
                                                                   results,
                                                                   ImmutableList.of(new PageInformation(0, 1L, 68L))
                                                               ),
                                                               null
    );
    Assert.assertEquals(
        objectMapper.writeValueAsString(expected),
        objectMapper.writeValueAsString(response.getEntity())
    );
  }

  @Test
  public void durableStorageDisabledTest()
  {
    NativeStatementResource resourceWithDurableStorage = new NativeStatementResource(
        objectMapper,
        smileMapper,
        indexingNativeServiceClient,
        createLifecycleFactory(),
        authorizerMapper,
        NilStorageConnector.getInstance()
    );

    MockHttpServletRequest testServletRequest = new MockHttpServletRequest();

    testServletRequest.setAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT, AUTHENTICATION_RESULT);
    testServletRequest.contentType = CONTENT_TYPE_JSON;

    SqlStatementResourceTest.assertExceptionMessage(
        doPost(
            testServletRequest,
            SIMPLE_SCAN_QUERY_WITH_DURABLE.getBytes(StandardCharsets.UTF_8),
            resourceWithDurableStorage
        ),
        DURABLE_STORAGE_UNAVAILABLE_MESSAGE,
        Response.Status.BAD_REQUEST
    );
  }

  @Test
  public void testWithDurableStorage() throws IOException
  {

    MockHttpServletRequest testServletRequest = new MockHttpServletRequest();

    testServletRequest.setAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT, AUTHENTICATION_RESULT);
    testServletRequest.contentType = CONTENT_TYPE_JSON;
    NativeStatementResult nativeStatementResult = (NativeStatementResult) doPost(
        SIMPLE_SCAN_QUERY_WITH_DURABLE_AND_RPP,
        testServletRequest
    ).getEntity();


    Assert.assertEquals(ImmutableList.of(
        new PageInformation(0, 2L, 120L, 0, 0),
        new PageInformation(1, 2L, 118L, 0, 1),
        new PageInformation(2, 2L, 122L, 0, 2)
    ), nativeStatementResult.getResultSetInformation().getPages());

    assertExpectedResults(
        "{\"cnt\":1,\"dim1\":\"\"}\n"
        + "{\"cnt\":1,\"dim1\":\"10.1\"}\n"
        + "{\"cnt\":1,\"dim1\":\"2\"}\n"
        + "{\"cnt\":1,\"dim1\":\"1\"}\n"
        + "{\"cnt\":1,\"dim1\":\"def\"}\n"
        + "{\"cnt\":1,\"dim1\":\"abc\"}\n"
        + "\n",
        resource.doGetResults(
            nativeStatementResult.getQueryId(),
            null,
            ResultFormat.OBJECTLINES.name(),
            SqlStatementResourceTest.makeOkRequest()
        ),
        objectMapper
    );

    assertExpectedResults(
        "{\"cnt\":1,\"dim1\":\"\"}\n"
        + "{\"cnt\":1,\"dim1\":\"10.1\"}\n"
        + "\n",
        resource.doGetResults(
            nativeStatementResult.getQueryId(),
            0L,
            ResultFormat.OBJECTLINES.name(),
            SqlStatementResourceTest.makeOkRequest()
        ),
        objectMapper
    );

    assertExpectedResults(
        "{\"cnt\":1,\"dim1\":\"def\"}\n"
        + "{\"cnt\":1,\"dim1\":\"abc\"}\n"
        + "\n",
        resource.doGetResults(
            nativeStatementResult.getQueryId(),
            2L,
            ResultFormat.OBJECTLINES.name(),
            SqlStatementResourceTest.makeOkRequest()
        ),
        objectMapper
    );
  }

  @Test
  public void testMultipleWorkersWithPageSizeLimiting() throws IOException
  {

    final File toRead = MSQTestFileUtils.getResourceAsTemporaryFile(temporaryFolder, this, "/wikipedia-sampled.json");
    final String toReadAsJson = toRead.getAbsolutePath();

    MockHttpServletRequest testServletRequest = new MockHttpServletRequest();

    testServletRequest.setAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT, AUTHENTICATION_RESULT);
    testServletRequest.contentType = CONTENT_TYPE_JSON;

    NativeStatementResult response = (NativeStatementResult) doPost(String.format(
        Locale.ROOT,
        MULTI_WORKER_SCAN_TEMPLATE,
        toReadAsJson,
        toReadAsJson
    ), testServletRequest).getEntity();

    Assert.assertEquals(ImmutableList.of(
        new PageInformation(0, 2L, 128L, 0, 0),
        new PageInformation(1, 2L, 132L, 1, 1),
        new PageInformation(2, 2L, 128L, 0, 2),
        new PageInformation(3, 2L, 132L, 1, 3),
        new PageInformation(4, 2L, 130L, 0, 4)
    ), response.getResultSetInformation().getPages());


    List<List<Object>> rows = new ArrayList<>();
    rows.add(ImmutableList.of(1466985600000L, "Lsjbot"));
    rows.add(ImmutableList.of(1466985600000L, "Lsjbot"));
    rows.add(ImmutableList.of(1466985600000L, "Beau.bot"));
    rows.add(ImmutableList.of(1466985600000L, "Beau.bot"));
    rows.add(ImmutableList.of(1466985600000L, "Lsjbot"));
    rows.add(ImmutableList.of(1466985600000L, "Lsjbot"));
    rows.add(ImmutableList.of(1466985600000L, "TaxonBot"));
    rows.add(ImmutableList.of(1466985600000L, "TaxonBot"));
    rows.add(ImmutableList.of(1466985600000L, "GiftBot"));
    rows.add(ImmutableList.of(1466985600000L, "GiftBot"));

    Assert.assertEquals(rows, SqlStatementResourceTest.getResultRowsFromResponse(resource.doGetResults(
        response.getQueryId(),
        null,
        ResultFormat.ARRAY.name(),
        SqlStatementResourceTest.makeOkRequest()
    )));

    Assert.assertEquals(rows.subList(0, 2), SqlStatementResourceTest.getResultRowsFromResponse(resource.doGetResults(
        response.getQueryId(),
        0L,
        ResultFormat.ARRAY.name(),
        SqlStatementResourceTest.makeOkRequest()
    )));

    Assert.assertEquals(rows.subList(2, 4), SqlStatementResourceTest.getResultRowsFromResponse(resource.doGetResults(
        response.getQueryId(),
        1L,
        ResultFormat.ARRAY.name(),
        SqlStatementResourceTest.makeOkRequest()
    )));

    Assert.assertEquals(rows.subList(4, 6), SqlStatementResourceTest.getResultRowsFromResponse(resource.doGetResults(
        response.getQueryId(),
        2L,
        ResultFormat.ARRAY.name(),
        SqlStatementResourceTest.makeOkRequest()
    )));

    Assert.assertEquals(rows.subList(6, 8), SqlStatementResourceTest.getResultRowsFromResponse(resource.doGetResults(
        response.getQueryId(),
        3L,
        ResultFormat.ARRAY.name(),
        SqlStatementResourceTest.makeOkRequest()
    )));

    Assert.assertEquals(rows.subList(8, 10), SqlStatementResourceTest.getResultRowsFromResponse(resource.doGetResults(
        response.getQueryId(),
        4L,
        ResultFormat.ARRAY.name(),
        SqlStatementResourceTest.makeOkRequest()
    )));
  }

  @Test
  public void testResultFormatWithParamInSelect() throws IOException
  {
    MockHttpServletRequest testServletRequest = new MockHttpServletRequest();

    testServletRequest.setAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT, AUTHENTICATION_RESULT);
    testServletRequest.contentType = CONTENT_TYPE_JSON;
    NativeStatementResult response = (NativeStatementResult) doPost(
        SIMPLE_SCAN_QUERY_WITH_DURABLE,
        testServletRequest
    ).getEntity();


    List<List<Object>> rows = new ArrayList<>();
    rows.add(ImmutableList.of(1, ""));
    rows.add(ImmutableList.of(1, "10.1"));
    rows.add(ImmutableList.of(1, "2"));
    rows.add(ImmutableList.of(1, "1"));
    rows.add(ImmutableList.of(1, "def"));
    rows.add(ImmutableList.of(1, "abc"));

    Assert.assertEquals(rows, SqlStatementResourceTest.getResultRowsFromResponse(resource.doGetResults(
        response.getQueryId(),
        null,
        ResultFormat.ARRAY.name(),
        SqlStatementResourceTest.makeOkRequest()
    )));

    Assert.assertEquals(rows, SqlStatementResourceTest.getResultRowsFromResponse(resource.doGetResults(
        response.getQueryId(),
        0L,
        ResultFormat.ARRAY.name(),
        SqlStatementResourceTest.makeOkRequest()
    )));
  }

  private Response doPost(String simpleScanQuery, MockHttpServletRequest testServletRequest)
  {
    return doPost(
        testServletRequest,
        simpleScanQuery.getBytes(StandardCharsets.UTF_8),
        resource
    );
  }

  private Response doPost(
      MockHttpServletRequest req,
      byte[] bytes,
      NativeStatementResource queryResource
  )
  {
    return queryResource.doPost(new ByteArrayInputStream(bytes), null, req);
  }

  private void assertExpectedResults(String expectedResult, Response resultsResponse, ObjectMapper objectMapper)
      throws IOException
  {
    byte[] bytes = responseToByteArray(resultsResponse, objectMapper);
    Assert.assertEquals(expectedResult, new String(bytes, StandardCharsets.UTF_8));
  }

}


