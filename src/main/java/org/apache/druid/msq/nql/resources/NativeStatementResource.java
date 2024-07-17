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

package org.apache.druid.msq.nql.resources;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.joda.ser.DateTimeSerializer;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.CountingOutputStream;
import com.google.inject.Inject;
import org.apache.druid.client.indexing.TaskPayloadResponse;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.Forbidden;
import org.apache.druid.error.NotFound;
import org.apache.druid.frame.Frame;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.AbstractStatementResource;
import org.apache.druid.msq.guice.MultiStageQuery;
import org.apache.druid.msq.indexing.MSQNativeControllerTask;
import org.apache.druid.msq.indexing.MSQSpec;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.nql.MSQNativeTaskQueryMaker;
import org.apache.druid.msq.nql.NativeStatementResult;
import org.apache.druid.msq.sql.StatementState;
import org.apache.druid.msq.util.AbstractResourceHelper;
import org.apache.druid.msq.util.NativeStatementResourceHelper;
import org.apache.druid.query.BadJsonQueryException;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryException;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.QueryLifecycle;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.QueryResponse;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.storage.StorageConnector;
import org.joda.time.DateTime;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;


@Path("/druid/v2/native/statements/")
public class NativeStatementResource extends AbstractStatementResource<NativeStatementResult, MSQNativeControllerTask>
{

  public static final String RESULT_FORMAT = "__resultFormat";
  private static final String DURABLE_ERROR_TEMPLATE =
      "The native statement api cannot read from the select destination [%s] provided "
      + "in the query context [%s] since it is not configured on the %s. It is recommended to configure durable storage "
      + "as it allows the user to fetch large result sets. Please contact your cluster admin to "
      + "configure durable storage.";
  private static final Logger log = new Logger(NativeStatementResource.class);
  protected final ObjectMapper objectMapper;
  protected final ObjectMapper smileMapper;
  private final QueryLifecycleFactory lifecycleFactory;
  private final AuthorizerMapper authorizerMapper;


  @Inject
  public NativeStatementResource(
      @Json final ObjectMapper jsonMapper,
      @Smile final ObjectMapper smileMapper,
      final OverlordClient overlordClient,
      final QueryLifecycleFactory lifecycleFactory,
      final AuthorizerMapper authorizerMapper,
      final @MultiStageQuery StorageConnector storageConnector
  )
  {
    super(jsonMapper, overlordClient, storageConnector);
    this.lifecycleFactory = lifecycleFactory;
    this.objectMapper = serializeDataTimeAsLong(jsonMapper);
    this.smileMapper = serializeDataTimeAsLong(smileMapper);
    this.authorizerMapper = authorizerMapper;
  }

  private static String getPreviousEtag(final HttpServletRequest req)
  {
    return req.getHeader("If-None-Match");
  }

  @VisibleForTesting
  static void resultPusherInternal(
      ResultFormat.Writer writer,
      Yielder<Object[]> yielder,
      List<String> names
  ) throws IOException
  {
    writer.writeResponseStart();

    while (!yielder.isDone()) {
      writer.writeRowStart();
      Object[] row = yielder.get();
      for (int i = 0; i < Math.min(names.size(), row.length); i++) {
        writer.writeRowField(
            names.get(i),
            row[i]
        );
      }
      writer.writeRowEnd();
      yielder = yielder.next(null);
    }
    writer.writeResponseEnd();
    yielder.close();
  }


  protected DruidException queryNotFoundException(String queryId)
  {
    return NotFound.exception(
        "Query [%s] was not found. The query details are no longer present or might not be of the type [%s]. Verify that the id is correct.",
        queryId,
        MSQNativeControllerTask.TYPE
    );
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response doPost(
      final InputStream in,
      @Context final HttpServletRequest req
  )
  {
    try {
      AuthorizationUtils.setRequestAuthorizationAttributeIfNeeded(req);
      final AuthenticationResult authenticationResult = AuthorizationUtils.authenticationResultFromRequest(req);
      final Query<?> query;
      try {
        query = readQuery(req, in);
      }
      catch (QueryException e) {
        return Response.serverError().tag(e.getMessage()).build();
      }

      final QueryContext context = new QueryContext(query.getContext());

      contextChecks(context);

      RowSignature signature = getRowSignature(query);
      MSQNativeTaskQueryMaker taskQueryMaker = new MSQNativeTaskQueryMaker(
          null,
          overlordClient,
          jsonMapper,
          getColumnMappings(signature),
          signature
      );

      QueryResponse<Object[]> response = taskQueryMaker.runNativeQuery(query);
      final Sequence<Object[]> sequence = response.getResults();

      return buildTaskResponse(sequence, authenticationResult);
    }
    catch (DruidException e) {
      return buildNonOkResponse(e);
    }
    catch (Exception e) {
      return buildNonOkResponse(
          DruidException.forPersona(DruidException.Persona.DEVELOPER)
                        .ofCategory(DruidException.Category.UNCATEGORIZED)
                        .build("%s", e.getMessage()));
    }
  }

  @GET
  @Path("/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response doGetStatus(
      @PathParam("id") final String queryId, @Context final HttpServletRequest req
  )
  {
    try {
      AuthorizationUtils.setRequestAuthorizationAttributeIfNeeded(req);
      final AuthenticationResult authenticationResult = AuthorizationUtils.authenticationResultFromRequest(req);

      Optional<NativeStatementResult> sqlStatementResult = getStatementStatus(
          queryId,
          authenticationResult,
          true,
          Action.READ
      );

      if (sqlStatementResult.isPresent()) {
        return Response.ok().entity(sqlStatementResult.get()).build();
      } else {
        throw queryNotFoundException(queryId);
      }
    }
    catch (DruidException e) {
      return buildNonOkResponse(e);
    }
    catch (ForbiddenException e) {
      log.debug("Got forbidden request for reason [%s]", e.getErrorMessage());
      return buildNonOkResponse(Forbidden.exception());
    }
    catch (Exception e) {
      log.warn(e, "Failed to handle query [%s]", queryId);
      return buildNonOkResponse(DruidException.forPersona(DruidException.Persona.DEVELOPER)
                                              .ofCategory(DruidException.Category.UNCATEGORIZED)
                                              .build(e, "Failed to handle query [%s]", queryId));
    }
  }

  @GET
  @Path("/{id}/results")
  @Produces(MediaType.APPLICATION_JSON)
  public Response doGetResults(
      @PathParam("id") final String queryId,
      @QueryParam("page") Long page,
      @QueryParam("resultFormat") String resultFormat,
      @Context final HttpServletRequest req
  )
  {
    try {
      AuthorizationUtils.setRequestAuthorizationAttributeIfNeeded(req);
      final AuthenticationResult authenticationResult = AuthorizationUtils.authenticationResultFromRequest(req);

      if (page != null && page < 0) {
        throw DruidException.forPersona(DruidException.Persona.USER)
                            .ofCategory(DruidException.Category.INVALID_INPUT)
                            .build(
                                "Page cannot be negative. Please pass a positive number."
                            );
      }

      TaskStatusResponse taskResponse = contactOverlord(overlordClient.taskStatus(queryId), queryId);
      if (taskResponse == null) {
        throw queryNotFoundException(queryId);
      }

      TaskStatusPlus statusPlus = taskResponse.getStatus();
      if (statusPlus == null || !MSQNativeControllerTask.TYPE.equals(statusPlus.getType())) {
        throw queryNotFoundException(queryId);
      }

      MSQNativeControllerTask msqControllerTask = getMSQControllerTaskAndCheckPermission(
          queryId,
          authenticationResult,
          Action.READ,
          authorizerMapper
      );
      throwIfQueryIsNotSuccessful(queryId, statusPlus);


      // returning results
      final Closer closer = Closer.create();
      final Optional<Yielder<Object[]>> results;
      final RowSignature signature = getRowSignature(msqControllerTask.getQuerySpec().getQuery());
      results = getResultYielder(queryId, page, msqControllerTask, closer);
      if (!results.isPresent()) {
        // no results, return empty
        return Response.ok().build();
      }

      ResultFormat preferredFormat = getPreferredResultFormat(resultFormat, msqControllerTask.getQuerySpec());
      return Response.ok((StreamingOutput) outputStream -> resultPusher(
          queryId,
          signature.getColumnNames(),
          closer,
          results,
          new CountingOutputStream(outputStream),
          preferredFormat
      )).build();


    }

    catch (DruidException e) {
      return buildNonOkResponse(e);
    }
    catch (ForbiddenException e) {
      log.debug("Got forbidden request for reason [%s]", e.getErrorMessage());
      return buildNonOkResponse(Forbidden.exception());
    }
    catch (Exception e) {
      log.warn(e, "Failed to handle query [%s]", queryId);
      return buildNonOkResponse(DruidException.forPersona(DruidException.Persona.DEVELOPER)
                                              .ofCategory(DruidException.Category.UNCATEGORIZED)
                                              .build(e, "Failed to handle query [%s]", queryId));
    }
  }

  @DELETE
  @Path("/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response deleteQuery(@PathParam("id") final String queryId, @Context final HttpServletRequest req)
  {

    try {
      AuthorizationUtils.setRequestAuthorizationAttributeIfNeeded(req);
      final AuthenticationResult authenticationResult = AuthorizationUtils.authenticationResultFromRequest(req);

      Optional<NativeStatementResult> nativeStatementResult = getStatementStatus(
          queryId,
          authenticationResult,
          false,
          Action.WRITE
      );
      if (nativeStatementResult.isPresent()) {
        switch (nativeStatementResult.get().getState()) {
          case ACCEPTED:
          case RUNNING:
            overlordClient.cancelTask(queryId);
            return Response.status(Response.Status.ACCEPTED).build();
          case SUCCESS:
          case FAILED:
            // we would also want to clean up the results in the future.
            return Response.ok().build();
          default:
            throw new ISE("Illegal State[%s] encountered", nativeStatementResult.get().getState());
        }
      } else {
        throw queryNotFoundException(queryId);
      }
    }
    catch (DruidException e) {
      return buildNonOkResponse(e);
    }
    catch (ForbiddenException e) {
      log.debug("Got forbidden request for reason [%s]", e.getErrorMessage());
      return buildNonOkResponse(Forbidden.exception());
    }
    catch (Exception e) {
      log.warn(e, "Failed to handle query [%s]", queryId);
      return buildNonOkResponse(DruidException.forPersona(DruidException.Persona.DEVELOPER)
                                              .ofCategory(DruidException.Category.UNCATEGORIZED)
                                              .build(e, "Failed to handle query [%s]", queryId));
    }
  }

  private RowSignature getRowSignature(Query<?> query)
  {
    QueryLifecycle lifecycle = lifecycleFactory.factorize();
    lifecycle.initialize(query);
    QueryToolChest<?, Query<?>> toolChest = lifecycle.getToolChest();
    return toolChest.resultArraySignature(query);
  }

  private ColumnMappings getColumnMappings(final RowSignature signature)
  {
    return ColumnMappings.identity(signature);
  }

  @Override
  protected MSQNativeControllerTask getTaskEntity(String queryId)
  {
    TaskPayloadResponse taskPayloadResponse = contactOverlord(overlordClient.taskPayload(queryId), queryId);
    NativeStatementResourceHelper.isMSQPayload(taskPayloadResponse, queryId);

    return (MSQNativeControllerTask) taskPayloadResponse.getPayload();
  }

  @Override
  protected Optional<NativeStatementResult> getStatementStatus(
      String queryId,
      AuthenticationResult authenticationResult,
      boolean withResults,
      Action forAction
  ) throws DruidException
  {
    TaskStatusResponse taskResponse = contactOverlord(overlordClient.taskStatus(queryId), queryId);
    if (taskResponse == null) {
      return Optional.empty();
    }

    TaskStatusPlus statusPlus = taskResponse.getStatus();
    if (statusPlus == null || !MSQNativeControllerTask.TYPE.equals(statusPlus.getType())) {
      return Optional.empty();
    }

    // since we need the controller payload for auth checks.
    MSQNativeControllerTask msqControllerTask = getMSQControllerTaskAndCheckPermission(
        queryId,
        authenticationResult,
        forAction,
        authorizerMapper
    );
    StatementState statementState = AbstractResourceHelper.getSqlStatementState(statusPlus);

    if (StatementState.FAILED == statementState) {
      return NativeStatementResourceHelper.getExceptionPayload(
          queryId,
          taskResponse,
          statusPlus,
          statementState,
          contactOverlord(overlordClient.taskReportAsMap(queryId), queryId)
      );
    } else {

      final Map<String, ColumnType> signature = NativeStatementResourceHelper.getColumnTypes(msqControllerTask.getSignature());
      return Optional.of(new NativeStatementResult(
          queryId,
          statementState,
          taskResponse.getStatus().getCreatedTime(),
          signature,
          taskResponse.getStatus().getDuration(),
          getResultSetInformation(
              queryId,
              msqControllerTask.getDataSource(),
              statementState,
              msqControllerTask.getQuerySpec().getDestination()
          ).orElse(null),
          null
      ));
    }
  }

  private void resultPusher(
      String queryId,
      List<String> names,
      Closer closer,
      Optional<Yielder<Object[]>> results,
      CountingOutputStream os,
      ResultFormat resultFormat
  ) throws IOException
  {
    try {
      try (final ResultFormat.Writer writer = resultFormat.createFormatter(os, jsonMapper)) {
        Yielder<Object[]> yielder = results.get();
        resultPusherInternal(writer, yielder, names);
      }
      catch (Exception e) {
        log.error(e, "Unable to stream results back for query[%s]", queryId);
        throw new ISE(e, "Unable to stream results back for query[%s]", queryId);
      }
    }
    catch (Exception e) {
      log.error(e, "Unable to stream results back for query[%s]", queryId);
      throw new ISE(e, "Unable to stream results back for query[%s]", queryId);
    }
    finally {
      closer.close();
    }
  }

  private ResultFormat getPreferredResultFormat(String resultFormatParam, MSQSpec msqSpec)
  {
    if (resultFormatParam == null) {
      return QueryContexts.getAsEnum(
          RESULT_FORMAT,
          msqSpec.getQuery().context().get(RESULT_FORMAT),
          ResultFormat.class,
          ResultFormat.DEFAULT_RESULT_FORMAT
      );
    }

    return QueryContexts.getAsEnum(
        "resultFormat",
        resultFormatParam,
        ResultFormat.class
    );
  }

  @Override
  protected Sequence<Object[]> getResultSequence(
      final StageDefinition finalStage,
      final Frame frame,
      MSQNativeControllerTask msqControllerTask
  )
  {
    return NativeStatementResourceHelper.getResultSequence(
        finalStage,
        frame,
        msqControllerTask.getQuerySpec().getColumnMappings()
    );
  }

  private Query<?> readQuery(
      final HttpServletRequest req,
      final InputStream in
  ) throws IOException
  {
    final Query<?> baseQuery;
    try {
      baseQuery = getRequestMapper(req).readValue(in, Query.class);
    }
    catch (JsonParseException e) {
      throw new BadJsonQueryException(e);
    }

    String prevEtag = getPreviousEtag(req);
    if (prevEtag == null) {
      return baseQuery;
    }

    return baseQuery.withOverriddenContext(
        QueryContexts.override(
            baseQuery.getContext(),
            "If-None-Match",
            prevEtag
        )
    );
  }

  protected ObjectMapper serializeDataTimeAsLong(ObjectMapper mapper)
  {
    return mapper.copy().registerModule(new SimpleModule().addSerializer(DateTime.class, new DateTimeSerializer()));
  }

  @Override
  protected String getDurableStorageErrorMsgTemplate()
  {
    return DURABLE_ERROR_TEMPLATE;
  }

  private ObjectMapper getRequestMapper(HttpServletRequest req)
  {
    String requestType = req.getContentType();
    return SmileMediaTypes.APPLICATION_JACKSON_SMILE.equals(requestType) ? smileMapper : objectMapper;
  }
}
