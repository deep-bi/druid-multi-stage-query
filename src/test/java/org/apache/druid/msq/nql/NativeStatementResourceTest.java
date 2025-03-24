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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.client.indexing.TaskPayloadResponse;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.msq.counters.CounterSnapshots;
import org.apache.druid.msq.counters.CounterSnapshotsTree;
import org.apache.druid.msq.indexing.MSQNativeControllerTask;
import org.apache.druid.msq.indexing.MSQSpec;
import org.apache.druid.msq.indexing.MSQTuningConfig;
import org.apache.druid.msq.indexing.destination.TaskReportMSQDestination;
import org.apache.druid.msq.indexing.report.MSQResultsReport;
import org.apache.druid.msq.indexing.report.MSQStagesReport;
import org.apache.druid.msq.indexing.report.MSQStatusReport;
import org.apache.druid.msq.indexing.report.MSQTaskReport;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.msq.indexing.report.MSQTaskReportTest;
import org.apache.druid.msq.nql.resources.NativeStatementResource;
import org.apache.druid.msq.sql.StatementState;
import org.apache.druid.msq.sql.entity.PageInformation;
import org.apache.druid.msq.sql.entity.ResultSetInformation;
import org.apache.druid.query.Query;
import org.apache.druid.rpc.HttpResponseException;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.mocks.MockHttpServletRequest;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.sql.http.SqlResourceTest;
import org.apache.druid.storage.local.LocalFileStorageConnector;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.druid.msq.sql.resources.SqlStatementResourceTest.ACCEPTED_SELECT_MSQ_QUERY;
import static org.apache.druid.msq.sql.resources.SqlStatementResourceTest.CREATED_TIME;
import static org.apache.druid.msq.sql.resources.SqlStatementResourceTest.ERRORED_SELECT_MSQ_QUERY;
import static org.apache.druid.msq.sql.resources.SqlStatementResourceTest.FAILURE_MSG;
import static org.apache.druid.msq.sql.resources.SqlStatementResourceTest.FINISHED_SELECT_MSQ_QUERY;
import static org.apache.druid.msq.sql.resources.SqlStatementResourceTest.RESULT_ROWS;
import static org.apache.druid.msq.sql.resources.SqlStatementResourceTest.RUNNING_SELECT_MSQ_QUERY;
import static org.apache.druid.msq.sql.resources.SqlStatementResourceTest.STATE_RW_USER;
import static org.apache.druid.msq.sql.resources.SqlStatementResourceTest.STATE_R_USER;
import static org.apache.druid.msq.sql.resources.SqlStatementResourceTest.STATE_W_USER;
import static org.apache.druid.msq.sql.resources.SqlStatementResourceTest.SUPERUSER;
import static org.apache.druid.msq.sql.resources.SqlStatementResourceTest.assertExceptionMessage;
import static org.apache.druid.msq.sql.resources.SqlStatementResourceTest.makeOkRequest;
import static org.apache.druid.msq.test.MSQTestOverlordServiceClient.QUEUE_INSERTION_TIME;

public class NativeStatementResourceTest extends NativeMSQTestBase
{


  private static final String SIMPLE_SCAN_QUERY = "{\n"
                                                  + "    \"queryType\": \"scan\",\n"
                                                  + "    \"dataSource\": \"target\",\n"
                                                  + "    \"granularity\": \"hour\",\n"
                                                  + "    \"resultFormat\":\"compactedList\",\n"
                                                  + "    \"legacy\":false,\n"
                                                  + "    \"intervals\": [\n"
                                                  + "      \"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"\n"
                                                  + "    ],\n"
                                                  + "    \"columns\": [\n"
                                                  + "      \"__time\",\n"
                                                  + "      \"alias\",\n"
                                                  + "      \"market\"\n"
                                                  + "    ],\n"
                                                  + "    \"context\":\n"
                                                  + "      {\n"
                                                  + "        \"__user\": \"allowAll\",\n"
                                                  + "        \"executionMode\": \"ASYNC\",\n"
                                                  + "        \"maxNumTasks\": 2,\n"
                                                  + "        \"scanSignature\": \"[{\\\"name\\\":\\\"__time\\\",\\\"type\\\":\\\"LONG\\\"},"
                                                  + "{\\\"name\\\":\\\"alias\\\",\\\"type\\\":\\\"STRING\\\"}, {\\\"name\\\":\\\"market\\\",\\\"type\\\":\\\"STRING\\\"}]\"\n"
                                                  + "      }\n"
                                                  + "}";
  private static final RowSignature ROW_SIGNATURE = RowSignature.builder()
                                                                .add(
                                                                    "__time",
                                                                    ColumnType.LONG
                                                                )
                                                                .add(
                                                                    "market",
                                                                    ColumnType.STRING
                                                                )
                                                                .add(
                                                                    "alias",
                                                                    ColumnType.STRING
                                                                )
                                                                .build();
  private final Supplier<MSQTaskReport> selectTaskReport = () -> new MSQTaskReport(
      FINISHED_SELECT_MSQ_QUERY,
      new MSQTaskReportPayload(
          new MSQStatusReport(
              TaskState.SUCCESS,
              null,
              new ArrayDeque<>(),
              null,
              0,
              new HashMap<>(),
              1,
              2,
              null,
              null
          ),
          MSQStagesReport.create(
              MSQTaskReportTest.QUERY_DEFINITION,
              ImmutableMap.of(),
              ImmutableMap.of(),
              ImmutableMap.of(0, 1),
              ImmutableMap.of(0, 1),
              ImmutableMap.of()
          ),
          CounterSnapshotsTree.fromMap(ImmutableMap.of(
              0,
              ImmutableMap.of(
                  0,
                  new CounterSnapshots(ImmutableMap.of(
                      "output",
                      new ChannelCounters.Snapshot(
                          new long[]{1L, 2L},
                          new long[]{3L, 5L},
                          new long[]{},
                          new long[]{},
                          new long[]{}
                      )
                  )
                  )
              )
          )),
          new MSQResultsReport(
              ImmutableList.of(
                  new MSQResultsReport.ColumnAndType(
                      "__time",
                      ColumnType.LONG
                  ),
                  new MSQResultsReport.ColumnAndType(
                      "alias",
                      ColumnType.STRING
                  ),
                  new MSQResultsReport.ColumnAndType(
                      "market",
                      ColumnType.STRING
                  )
              ),
              ImmutableList.of(
                  SqlTypeName.TIMESTAMP,
                  SqlTypeName.VARCHAR,
                  SqlTypeName.VARCHAR
              ),
              RESULT_ROWS,
              null
          )
      )
  );
  private final AuthorizerMapper authorizerMapper = new AuthorizerMapper(null)
  {
    @Override
    public Authorizer getAuthorizer(String name)
    {
      return (authenticationResult, resource, action) -> {
        if (SUPERUSER.equals(authenticationResult.getIdentity())) {
          return Access.OK;
        }

        switch (resource.getType()) {
          case ResourceType.DATASOURCE:
          case ResourceType.VIEW:
          case ResourceType.QUERY_CONTEXT:
          case ResourceType.EXTERNAL:
            return Access.OK;
          case ResourceType.STATE:
            String identity = authenticationResult.getIdentity();
            if (action == Action.READ) {
              if (STATE_R_USER.equals(identity) || STATE_RW_USER.equals(identity)) {
                return Access.OK;
              }
            } else if (action == Action.WRITE) {
              if (STATE_W_USER.equals(identity) || STATE_RW_USER.equals(identity)) {
                return Access.OK;
              }
            }
            return Access.DENIED;

          default:
            return Access.DENIED;
        }
      };
    }
  };
  private NativeStatementResource resource;
  @Mock
  private OverlordClient overlordClient;

  public static Map<String, ColumnType> generateRowSignature()
  {
    Map<String, ColumnType> rowSignature = new HashMap<>();
    rowSignature.put("__time", ColumnType.LONG);
    rowSignature.put("alias", ColumnType.STRING);
    rowSignature.put("market", ColumnType.STRING);
    return rowSignature;
  }

  private static MockHttpServletRequest makeExpectedReq(AuthenticationResult authenticationResult)
  {
    MockHttpServletRequest req = new MockHttpServletRequest();
    req.attributes.put(AuthConfig.DRUID_AUTHENTICATION_RESULT, authenticationResult);
    req.remoteAddr = "1.2.3.4";
    return req;
  }

  private static AuthenticationResult makeAuthResultForUser(String user)
  {
    return new AuthenticationResult(
        user,
        AuthConfig.ALLOW_ALL_NAME,
        null,
        null
    );
  }

  public MSQNativeControllerTask getMSQSelectPayload() throws JsonProcessingException
  {
    return new MSQNativeControllerTask(
        ACCEPTED_SELECT_MSQ_QUERY,
        MSQSpec.builder()
               .query(objectMapper.readValue(SIMPLE_SCAN_QUERY, Query.class))
               .columnMappings(
                   ColumnMappings.identity(ROW_SIGNATURE))
               .destination(TaskReportMSQDestination.instance())
               .tuningConfig(
                   MSQTuningConfig.defaultConfig())
               .build(),
        new HashMap<>(),
        ROW_SIGNATURE
    );
  }

  @BeforeEach
  public void init() throws IOException
  {
    overlordClient = Mockito.mock(OverlordClient.class);
    setupMocks(overlordClient);
    resource = new NativeStatementResource(
        objectMapper,
        smileMapper,
        overlordClient,
        createLifecycleFactory(),
        authorizerMapper,
        new LocalFileStorageConnector(newTempFolder("local"))
    );
  }

  private void setupMocks(OverlordClient indexingServiceClient) throws JsonProcessingException
  {
    Mockito.when(indexingServiceClient.taskStatus(ArgumentMatchers.eq(ACCEPTED_SELECT_MSQ_QUERY)))
           .thenReturn(Futures.immediateFuture(new TaskStatusResponse(ACCEPTED_SELECT_MSQ_QUERY, new TaskStatusPlus(
               ACCEPTED_SELECT_MSQ_QUERY,
               null,
               MSQNativeControllerTask.TYPE,
               CREATED_TIME,
               QUEUE_INSERTION_TIME,
               null,
               null,
               null,
               TaskLocation.unknown(),
               null,
               null
           ))));

    Mockito.when(indexingServiceClient.taskPayload(ArgumentMatchers.eq(ACCEPTED_SELECT_MSQ_QUERY)))
           .thenReturn(Futures.immediateFuture(new TaskPayloadResponse(
               ACCEPTED_SELECT_MSQ_QUERY,
               getMSQSelectPayload()
           )));

    Mockito.when(indexingServiceClient.taskStatus(ArgumentMatchers.eq(RUNNING_SELECT_MSQ_QUERY)))
           .thenReturn(Futures.immediateFuture(new TaskStatusResponse(RUNNING_SELECT_MSQ_QUERY, new TaskStatusPlus(
               RUNNING_SELECT_MSQ_QUERY,
               null,
               MSQNativeControllerTask.TYPE,
               CREATED_TIME,
               QUEUE_INSERTION_TIME,
               TaskState.RUNNING,
               null,
               null,
               TaskLocation.create("test", 0, 0),
               null,
               null
           ))));

    Mockito.when(indexingServiceClient.taskPayload(RUNNING_SELECT_MSQ_QUERY))
           .thenReturn(Futures.immediateFuture(new TaskPayloadResponse(
               RUNNING_SELECT_MSQ_QUERY,
               getMSQSelectPayload()
           )));

    Mockito.when(indexingServiceClient.taskStatus(ArgumentMatchers.eq(FINISHED_SELECT_MSQ_QUERY)))
           .thenReturn(Futures.immediateFuture(new TaskStatusResponse(FINISHED_SELECT_MSQ_QUERY, new TaskStatusPlus(
               FINISHED_SELECT_MSQ_QUERY,
               null,
               MSQNativeControllerTask.TYPE,
               CREATED_TIME,
               QUEUE_INSERTION_TIME,
               TaskState.SUCCESS,
               null,
               100L,
               TaskLocation.create("test", 0, 0),
               null,
               null
           ))));

    Mockito.when(indexingServiceClient.taskPayload(FINISHED_SELECT_MSQ_QUERY))
           .thenReturn(Futures.immediateFuture(new TaskPayloadResponse(
               FINISHED_SELECT_MSQ_QUERY,
               getMSQSelectPayload()
           )));


    Mockito.when(indexingServiceClient.taskReportAsMap(FINISHED_SELECT_MSQ_QUERY))
           .thenAnswer(inv -> Futures.immediateFuture(TaskReport.buildTaskReports(selectTaskReport.get())));

    Mockito.when(indexingServiceClient.taskStatus(ArgumentMatchers.eq(ERRORED_SELECT_MSQ_QUERY)))
           .thenReturn(Futures.immediateFuture(new TaskStatusResponse(ERRORED_SELECT_MSQ_QUERY, new TaskStatusPlus(
               ERRORED_SELECT_MSQ_QUERY,
               null,
               MSQNativeControllerTask.TYPE,
               CREATED_TIME,
               QUEUE_INSERTION_TIME,
               TaskState.FAILED,
               null,
               -1L,
               TaskLocation.unknown(),
               null,
               FAILURE_MSG
           ))));

    Mockito.when(indexingServiceClient.taskPayload(ArgumentMatchers.eq(ERRORED_SELECT_MSQ_QUERY)))
           .thenReturn(Futures.immediateFuture(new TaskPayloadResponse(
               ERRORED_SELECT_MSQ_QUERY,
               getMSQSelectPayload()
           )));

    Mockito.when(indexingServiceClient.taskReportAsMap(ArgumentMatchers.eq(ERRORED_SELECT_MSQ_QUERY)))
           .thenReturn(Futures.immediateFuture(null));

  }

  @Test
  public void testMSQSelectAcceptedQuery()
  {
    Response response = resource.doGetStatus(ACCEPTED_SELECT_MSQ_QUERY, false, true, makeOkRequest());
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Assertions.assertEquals(
        new NativeStatementResult(
            ACCEPTED_SELECT_MSQ_QUERY,
            StatementState.ACCEPTED,
            CREATED_TIME,
            generateRowSignature(),
            null,
            null,
            null
        ),
        response.getEntity()
    );

    assertExceptionMessage(
        resource.doGetResults(ACCEPTED_SELECT_MSQ_QUERY, 0L, null, makeOkRequest()),
        StringUtils.format(
            "Query[%s] is currently in [%s] state. Please wait for it to complete.",
            ACCEPTED_SELECT_MSQ_QUERY,
            StatementState.ACCEPTED
        ),
        Response.Status.BAD_REQUEST
    );
    Assertions.assertEquals(
        Response.Status.ACCEPTED.getStatusCode(),
        resource.deleteQuery(ACCEPTED_SELECT_MSQ_QUERY, makeOkRequest()).getStatus()
    );
  }

  @Test
  public void testMSQSelectRunningQuery()
  {

    Response response = resource.doGetStatus(RUNNING_SELECT_MSQ_QUERY, false, true, makeOkRequest());
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Assertions.assertEquals(
        new NativeStatementResult(
            RUNNING_SELECT_MSQ_QUERY,
            StatementState.RUNNING,
            CREATED_TIME,
            generateRowSignature(),
            null,
            null,
            null
        ),
        response.getEntity()
    );

    assertExceptionMessage(
        resource.doGetResults(RUNNING_SELECT_MSQ_QUERY, 0L, null, makeOkRequest()),
        StringUtils.format(
            "Query[%s] is currently in [%s] state. Please wait for it to complete.",
            RUNNING_SELECT_MSQ_QUERY,
            StatementState.RUNNING
        ),
        Response.Status.BAD_REQUEST
    );
    Assertions.assertEquals(
        Response.Status.ACCEPTED.getStatusCode(),
        resource.deleteQuery(RUNNING_SELECT_MSQ_QUERY, makeOkRequest()).getStatus()
    );
  }

  @Test
  public void testFinishedSelectMSQQuery() throws Exception
  {
    Response response = resource.doGetStatus(FINISHED_SELECT_MSQ_QUERY, false, true, makeOkRequest());
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Assertions.assertEquals(objectMapper.writeValueAsString(new NativeStatementResult(
        FINISHED_SELECT_MSQ_QUERY,
        StatementState.SUCCESS,
        CREATED_TIME,
        generateRowSignature(),
        100L,
        new ResultSetInformation(
            3L,
            8L,
            null,
            MSQNativeControllerTask.DUMMY_DATASOURCE_FOR_SELECT,
            RESULT_ROWS,
            ImmutableList.of(new PageInformation(0, 3L, 8L))
        ),
        null
    )), objectMapper.writeValueAsString(response.getEntity()));

    Response resultsResponse = resource.doGetResults(
        FINISHED_SELECT_MSQ_QUERY,
        0L,
        ResultFormat.OBJECTLINES.name(),
        makeOkRequest()
    );
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resultsResponse.getStatus());

    String expectedResult = "{\"__time\":123,\"alias\":\"foo\",\"market\":\"bar\"}\n"
                            + "{\"__time\":234,\"alias\":\"foo1\",\"market\":\"bar1\"}\n\n";

    assertExpectedResults(expectedResult, resultsResponse);

    Assertions.assertEquals(
        Response.Status.OK.getStatusCode(),
        resource.deleteQuery(FINISHED_SELECT_MSQ_QUERY, makeOkRequest()).getStatus()
    );

    assertExpectedResults(
        expectedResult,
        resource.doGetResults(
            FINISHED_SELECT_MSQ_QUERY,
            0L,
            ResultFormat.OBJECTLINES.name(),
            makeOkRequest()
        )
    );

    assertExpectedResults(
        expectedResult,
        resource.doGetResults(
            FINISHED_SELECT_MSQ_QUERY,
            null,
            ResultFormat.OBJECTLINES.name(),
            makeOkRequest()
        )
    );

    Assertions.assertEquals(
        Response.Status.BAD_REQUEST.getStatusCode(),
        resource.doGetResults(FINISHED_SELECT_MSQ_QUERY, -1L, null, makeOkRequest()).getStatus()
    );
  }

  @Test
  public void testFailedMSQQuery()
  {
    assertExceptionMessage(
        resource.doGetStatus(ERRORED_SELECT_MSQ_QUERY, false, true, makeOkRequest()),
        FAILURE_MSG,
        Response.Status.OK
    );
    assertExceptionMessage(
        resource.doGetResults(ERRORED_SELECT_MSQ_QUERY, 0L, null, makeOkRequest()),
        StringUtils.format(
            "Query[%s] failed. Check the status api for more details.",
            ERRORED_SELECT_MSQ_QUERY
        ),
        Response.Status.BAD_REQUEST
    );

    Assertions.assertEquals(
        Response.Status.OK.getStatusCode(),
        resource.deleteQuery(ERRORED_SELECT_MSQ_QUERY, makeOkRequest()).getStatus()
    );
  }

  @Test
  public void testAPIBehaviourWithSuperUsers()
  {
    Assertions.assertEquals(
        Response.Status.OK.getStatusCode(),
        resource.doGetStatus(
            RUNNING_SELECT_MSQ_QUERY,
            false, true, makeExpectedReq(makeAuthResultForUser(SUPERUSER))
        ).getStatus()
    );
    Assertions.assertEquals(
        Response.Status.BAD_REQUEST.getStatusCode(),
        resource.doGetResults(
            RUNNING_SELECT_MSQ_QUERY,
            1L,
            null,
            makeExpectedReq(makeAuthResultForUser(SUPERUSER))
        ).getStatus()
    );
    Assertions.assertEquals(
        Response.Status.ACCEPTED.getStatusCode(),
        resource.deleteQuery(
            RUNNING_SELECT_MSQ_QUERY,
            makeExpectedReq(makeAuthResultForUser(SUPERUSER))
        ).getStatus()
    );
  }

  @Test
  public void testAPIBehaviourWithDifferentUserAndNoStatePermission()
  {
    AuthenticationResult differentUserAuthResult = makeAuthResultForUser("differentUser");
    Assertions.assertEquals(
        Response.Status.FORBIDDEN.getStatusCode(),
        resource.doGetStatus(
            RUNNING_SELECT_MSQ_QUERY,
            false, true, makeExpectedReq(differentUserAuthResult)
        ).getStatus()
    );
    Assertions.assertEquals(
        Response.Status.FORBIDDEN.getStatusCode(),
        resource.doGetResults(
            RUNNING_SELECT_MSQ_QUERY,
            1L,
            null,
            makeExpectedReq(differentUserAuthResult)
        ).getStatus()
    );
    Assertions.assertEquals(
        Response.Status.FORBIDDEN.getStatusCode(),
        resource.deleteQuery(
            RUNNING_SELECT_MSQ_QUERY,
            makeExpectedReq(differentUserAuthResult)
        ).getStatus()
    );
  }

  @Test
  public void testAPIBehaviourWithDifferentUserAndStateRPermission()
  {
    AuthenticationResult differentUserAuthResult = makeAuthResultForUser(STATE_R_USER);
    Assertions.assertEquals(
        Response.Status.OK.getStatusCode(),
        resource.doGetStatus(
            RUNNING_SELECT_MSQ_QUERY,
            false, true, makeExpectedReq(differentUserAuthResult)
        ).getStatus()
    );
    Assertions.assertEquals(
        Response.Status.BAD_REQUEST.getStatusCode(),
        resource.doGetResults(
            RUNNING_SELECT_MSQ_QUERY,
            1L,
            null,
            makeExpectedReq(differentUserAuthResult)
        ).getStatus()
    );
    Assertions.assertEquals(
        Response.Status.FORBIDDEN.getStatusCode(),
        resource.deleteQuery(
            RUNNING_SELECT_MSQ_QUERY,
            makeExpectedReq(differentUserAuthResult)
        ).getStatus()
    );
  }

  @Test
  public void testAPIBehaviourWithDifferentUserAndStateWPermission()
  {
    AuthenticationResult differentUserAuthResult = makeAuthResultForUser(STATE_W_USER);
    Assertions.assertEquals(
        Response.Status.FORBIDDEN.getStatusCode(),
        resource.doGetStatus(
            RUNNING_SELECT_MSQ_QUERY,
            false, true, makeExpectedReq(differentUserAuthResult)
        ).getStatus()
    );
    Assertions.assertEquals(
        Response.Status.FORBIDDEN.getStatusCode(),
        resource.doGetResults(
            RUNNING_SELECT_MSQ_QUERY,
            1L,
            null,
            makeExpectedReq(differentUserAuthResult)
        ).getStatus()
    );
    Assertions.assertEquals(
        Response.Status.ACCEPTED.getStatusCode(),
        resource.deleteQuery(
            RUNNING_SELECT_MSQ_QUERY,
            makeExpectedReq(differentUserAuthResult)
        ).getStatus()
    );
  }

  @Test
  public void testAPIBehaviourWithDifferentUserAndStateRWPermission()
  {
    AuthenticationResult differentUserAuthResult = makeAuthResultForUser(STATE_RW_USER);
    Assertions.assertEquals(
        Response.Status.OK.getStatusCode(),
        resource.doGetStatus(
            RUNNING_SELECT_MSQ_QUERY,
            false, true, makeExpectedReq(differentUserAuthResult)
        ).getStatus()
    );
    Assertions.assertEquals(
        Response.Status.BAD_REQUEST.getStatusCode(),
        resource.doGetResults(
            RUNNING_SELECT_MSQ_QUERY,
            1L,
            null,
            makeExpectedReq(differentUserAuthResult)
        ).getStatus()
    );
    Assertions.assertEquals(
        Response.Status.ACCEPTED.getStatusCode(),
        resource.deleteQuery(
            RUNNING_SELECT_MSQ_QUERY,
            makeExpectedReq(differentUserAuthResult)
        ).getStatus()
    );
  }

  @Test
  public void testTaskIdNotFound()
  {
    String taskIdNotFound = "notFound";
    final DefaultHttpResponse incorrectResponse =
        new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND);
    SettableFuture<TaskStatusResponse> settableFuture = SettableFuture.create();
    settableFuture.setException(new HttpResponseException(new StringFullResponseHolder(
        incorrectResponse,
        StandardCharsets.UTF_8
    )));
    Mockito.when(overlordClient.taskStatus(taskIdNotFound)).thenReturn(settableFuture);

    Assertions.assertEquals(
        Response.Status.NOT_FOUND.getStatusCode(),
        resource.doGetStatus(taskIdNotFound, false, true, makeOkRequest()).getStatus()
    );
    Assertions.assertEquals(
        Response.Status.NOT_FOUND.getStatusCode(),
        resource.doGetResults(taskIdNotFound, null, null, makeOkRequest()).getStatus()
    );
    Assertions.assertEquals(
        Response.Status.NOT_FOUND.getStatusCode(),
        resource.deleteQuery(taskIdNotFound, makeOkRequest()).getStatus()
    );
  }

  private void assertExpectedResults(String expectedResult, Response resultsResponse) throws IOException
  {
    byte[] bytes = SqlResourceTest.responseToByteArray(resultsResponse);
    Assertions.assertEquals(expectedResult, new String(bytes, StandardCharsets.UTF_8));
  }
}
