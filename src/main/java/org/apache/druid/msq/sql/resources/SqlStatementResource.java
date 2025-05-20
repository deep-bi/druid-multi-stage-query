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

package org.apache.druid.msq.sql.resources;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CountingOutputStream;
import com.google.inject.Inject;
import org.apache.druid.client.indexing.TaskPayloadResponse;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.Forbidden;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.error.NotFound;
import org.apache.druid.error.QueryExceptionCompat;
import org.apache.druid.frame.Frame;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.AbstractStatementResource;
import org.apache.druid.msq.exec.ResultsContext;
import org.apache.druid.msq.guice.MultiStageQuery;
import org.apache.druid.msq.indexing.MSQControllerTask;
import org.apache.druid.msq.indexing.MSQSpec;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.sql.MSQTaskSqlEngine;
import org.apache.druid.msq.sql.StatementState;
import org.apache.druid.msq.sql.entity.ColumnNameAndTypes;
import org.apache.druid.msq.sql.entity.SqlStatementResult;
import org.apache.druid.msq.util.AbstractResourceHelper;
import org.apache.druid.msq.util.SqlStatementResourceHelper;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryException;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.server.QueryResponse;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.sql.DirectStatement;
import org.apache.druid.sql.HttpStatement;
import org.apache.druid.sql.SqlRowTransformer;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.sql.http.SqlQuery;
import org.apache.druid.sql.http.SqlResource;
import org.apache.druid.storage.StorageConnectorProvider;

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
import java.util.List;
import java.util.Map;
import java.util.Optional;


@Path("/druid/v2/sql/statements/")
public class SqlStatementResource extends AbstractStatementResource<SqlStatementResult, MSQControllerTask>
{

  public static final String RESULT_FORMAT = "__resultFormat";
  private static final String DURABLE_ERROR_TEMPLATE =
      "The sql statement api cannot read from the select destination [%s] provided "
      + "in the query context [%s] since it is not configured on the %s. It is recommended to configure durable storage "
      + "as it allows the user to fetch large result sets. Please contact your cluster admin to "
      + "configure durable storage.";
  private static final Logger log = new Logger(SqlStatementResource.class);
  private final SqlStatementFactory msqSqlStatementFactory;
  private final AuthorizerMapper authorizerMapper;


  @Inject
  public SqlStatementResource(
      final @MultiStageQuery SqlStatementFactory msqSqlStatementFactory,
      final ObjectMapper jsonMapper,
      final OverlordClient overlordClient,
      final @MultiStageQuery StorageConnectorProvider storageConnectorProvider,
      final AuthorizerMapper authorizerMapper
  )
  {
    super(jsonMapper, overlordClient, storageConnectorProvider.createStorageConnector(null));
    this.msqSqlStatementFactory = msqSqlStatementFactory;
    this.authorizerMapper = authorizerMapper;
  }

  @VisibleForTesting
  static void resultPusherInternal(
      ResultFormat.Writer writer,
      Yielder<Object[]> yielder,
      List<ColumnNameAndTypes> rowSignature
  ) throws IOException
  {
    writer.writeResponseStart();

    while (!yielder.isDone()) {
      writer.writeRowStart();
      Object[] row = yielder.get();
      for (int i = 0; i < Math.min(rowSignature.size(), row.length); i++) {
        writer.writeRowField(
            rowSignature.get(i).getColName(),
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
        MSQControllerTask.TYPE
    );
  }

  /**
   * API for clients like web-console to check if this resource is enabled.
   */

  @GET
  @Path("/enabled")
  @Produces(MediaType.APPLICATION_JSON)
  public Response isEnabled(@Context final HttpServletRequest request)
  {
    // All authenticated users are authorized for this API.
    AuthorizationUtils.setRequestAuthorizationAttributeIfNeeded(request);

    return Response.ok(ImmutableMap.of("enabled", true)).build();
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response doPost(final SqlQuery sqlQuery, @Context final HttpServletRequest req)
  {
    SqlQuery modifiedQuery = createModifiedSqlQuery(sqlQuery);

    final HttpStatement stmt = msqSqlStatementFactory.httpStatement(modifiedQuery, req);
    final String sqlQueryId = stmt.sqlQueryId();
    final String currThreadName = Thread.currentThread().getName();
    boolean isDebug = false;
    try {
      QueryContext queryContext = QueryContext.of(modifiedQuery.getContext());
      isDebug = queryContext.isDebug();
      contextChecks(queryContext);

      Thread.currentThread().setName(StringUtils.format("statement_sql[%s]", sqlQueryId));

      final DirectStatement.ResultSet plan = stmt.plan();
      // in case the engine is async, the query is not run yet. We just return the taskID in case of non explain queries.
      final QueryResponse<Object[]> response = plan.run();
      final Sequence<Object[]> sequence = response.getResults();
      final SqlRowTransformer rowTransformer = plan.createRowTransformer();

      final boolean isTaskStruct = MSQTaskSqlEngine.TASK_STRUCT_FIELD_NAMES.equals(rowTransformer.getFieldList());

      if (isTaskStruct) {
        return buildTaskResponse(sequence, stmt.query().authResult());
      } else {
        // Used for EXPLAIN
        return buildStandardResponse(sequence, modifiedQuery, sqlQueryId, rowTransformer);
      }
    }
    catch (DruidException e) {
      stmt.reporter().failed(e);
      return buildNonOkResponse(e);
    }
    catch (QueryException queryException) {
      stmt.reporter().failed(queryException);
      final DruidException underlyingException = DruidException.fromFailure(new QueryExceptionCompat(queryException));
      return buildNonOkResponse(underlyingException);
    }
    catch (ForbiddenException e) {
      log.debug("Got forbidden request for reason [%s]", e.getErrorMessage());
      return buildNonOkResponse(Forbidden.exception());
    }
    // Calcite throws java.lang.AssertionError at various points in planning/validation.
    catch (AssertionError | Exception e) {
      stmt.reporter().failed(e);
      if (isDebug) {
        log.warn(e, "Failed to handle query [%s]", sqlQueryId);
      } else {
        log.noStackTrace().warn(e, "Failed to handle query [%s]", sqlQueryId);
      }
      return buildNonOkResponse(
          DruidException.forPersona(DruidException.Persona.DEVELOPER)
                        .ofCategory(DruidException.Category.UNCATEGORIZED)
                        .build("%s", e.getMessage())
      );
    }
    finally {
      stmt.close();
      Thread.currentThread().setName(currThreadName);
    }
  }

  @GET
  @Path("/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response doGetStatus(
      @PathParam("id") final String queryId,
      @QueryParam("detail") boolean detail,
      @Context final HttpServletRequest req
  )
  {
    try {
      AuthorizationUtils.setRequestAuthorizationAttributeIfNeeded(req);
      final AuthenticationResult authenticationResult = AuthorizationUtils.authenticationResultFromRequest(req);

      Optional<SqlStatementResult> sqlStatementResult = getStatementStatus(
          queryId,
          authenticationResult,
          true,
          Action.READ,
          detail
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
      if (statusPlus == null || !MSQControllerTask.TYPE.equals(statusPlus.getType())) {
        throw queryNotFoundException(queryId);
      }

      MSQControllerTask msqControllerTask = getMSQControllerTaskAndCheckPermission(
          queryId,
          authenticationResult,
          Action.READ,
          authorizerMapper
      );
      throwIfQueryIsNotSuccessful(queryId, statusPlus);

      Optional<List<ColumnNameAndTypes>> signature = SqlStatementResourceHelper.getSignature(msqControllerTask);
      if (!signature.isPresent() || MSQControllerTask.isIngestion(msqControllerTask.getQuerySpec())) {
        // Since it's not a select query, nothing to return.
        return Response.ok().build();
      }

      // returning results
      final Closer closer = Closer.create();
      final Optional<Yielder<Object[]>> results;
      results = getResultYielder(queryId, page, msqControllerTask, closer);
      if (!results.isPresent()) {
        // no results, return empty
        return Response.ok().build();
      }

      ResultFormat preferredFormat = getPreferredResultFormat(resultFormat, msqControllerTask.getQuerySpec());
      return Response.ok((StreamingOutput) outputStream -> resultPusher(
          queryId,
          signature,
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

  /**
   * Queries can be canceled while in any {@link StatementState}. Canceling a query that has already completed will be a no-op.
   *
   * @param queryId queryId
   * @param req     httpServletRequest
   * @return HTTP 404 if the query ID does not exist,expired or originated by different user. HTTP 202 if the deletion
   * request has been accepted.
   */
  @DELETE
  @Path("/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response deleteQuery(@PathParam("id") final String queryId, @Context final HttpServletRequest req)
  {

    try {
      AuthorizationUtils.setRequestAuthorizationAttributeIfNeeded(req);
      final AuthenticationResult authenticationResult = AuthorizationUtils.authenticationResultFromRequest(req);

      Optional<SqlStatementResult> sqlStatementResult = getStatementStatus(
          queryId,
          authenticationResult,
          false,
          Action.WRITE,
          false
      );
      if (sqlStatementResult.isPresent()) {
        switch (sqlStatementResult.get().getState()) {
          case ACCEPTED:
          case RUNNING:
            overlordClient.cancelTask(queryId);
            return Response.status(Response.Status.ACCEPTED).build();
          case SUCCESS:
          case FAILED:
            // we would also want to clean up the results in the future.
            return Response.ok().build();
          default:
            throw new ISE("Illegal State[%s] encountered", sqlStatementResult.get().getState());
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

  private Response buildStandardResponse(
      Sequence<Object[]> sequence,
      SqlQuery sqlQuery,
      String sqlQueryId,
      SqlRowTransformer rowTransformer
  ) throws IOException
  {
    final Yielder<Object[]> yielder0 = Yielders.each(sequence);

    try {
      final Response.ResponseBuilder responseBuilder = Response.ok((StreamingOutput) outputStream -> {
        CountingOutputStream os = new CountingOutputStream(outputStream);
        Yielder<Object[]> yielder = yielder0;

        try (final ResultFormat.Writer writer = sqlQuery.getResultFormat().createFormatter(os, jsonMapper)) {
          writer.writeResponseStart();

          if (sqlQuery.includeHeader()) {
            writer.writeHeader(
                rowTransformer.getRowType(),
                sqlQuery.includeTypesHeader(),
                sqlQuery.includeSqlTypesHeader()
            );
          }

          while (!yielder.isDone()) {
            final Object[] row = yielder.get();
            writer.writeRowStart();
            for (int i = 0; i < rowTransformer.getFieldList().size(); i++) {
              final Object value = rowTransformer.transform(row, i);
              writer.writeRowField(rowTransformer.getFieldList().get(i), value);
            }
            writer.writeRowEnd();
            yielder = yielder.next(null);
          }

          writer.writeResponseEnd();
        }
        catch (Exception e) {
          log.error(e, "Unable to send SQL response [%s]", sqlQueryId);
          throw new RuntimeException(e);
        }
        finally {
          yielder.close();
        }
      });

      if (sqlQuery.includeHeader()) {
        responseBuilder.header(SqlResource.SQL_HEADER_RESPONSE_HEADER, SqlResource.SQL_HEADER_VALUE);
      }

      return responseBuilder.build();
    }
    catch (Throwable e) {
      // make sure to close yielder if anything happened before starting to serialize the response.
      yielder0.close();
      throw e;
    }
  }

  @Override
  protected MSQControllerTask getTaskEntity(final String queryId)
  {
    TaskPayloadResponse taskPayloadResponse = contactOverlord(overlordClient.taskPayload(queryId), queryId);
    SqlStatementResourceHelper.isMSQPayload(taskPayloadResponse, queryId);

    return (MSQControllerTask) taskPayloadResponse.getPayload();
  }

  @Override
  protected Optional<SqlStatementResult> getStatementStatus(
      String queryId,
      AuthenticationResult authenticationResult,
      boolean withResults,
      Action forAction,
      boolean detail
  ) throws DruidException
  {
    TaskStatusResponse taskResponse = contactOverlord(overlordClient.taskStatus(queryId), queryId);
    if (taskResponse == null) {
      return Optional.empty();
    }

    TaskStatusPlus statusPlus = taskResponse.getStatus();
    if (statusPlus == null || !MSQControllerTask.TYPE.equals(statusPlus.getType())) {
      return Optional.empty();
    }

    // since we need the controller payload for auth checks.
    MSQControllerTask msqControllerTask = getMSQControllerTaskAndCheckPermission(
        queryId,
        authenticationResult,
        forAction,
        authorizerMapper
    );
    StatementState statementState = AbstractResourceHelper.getSqlStatementState(statusPlus);

    MSQTaskReportPayload taskReportPayload = null;
    if (detail || StatementState.FAILED == statementState) {
      try {
        taskReportPayload = SqlStatementResourceHelper.getPayload(
            contactOverlord(overlordClient.taskReportAsMap(queryId), queryId)
        );
      }
      catch (DruidException e) {
        if (!e.getErrorCode().equals("notFound") && !e.getMessage().contains("Unable to contact overlord")) {
          throw e;
        }
      }
    }

    if (StatementState.FAILED == statementState) {
      return SqlStatementResourceHelper.getExceptionPayload(
          queryId,
          taskResponse,
          statusPlus,
          statementState,
          taskReportPayload,
          jsonMapper,
          detail
      );
    } else {
      Optional<List<ColumnNameAndTypes>> signature = SqlStatementResourceHelper.getSignature(msqControllerTask);
      return Optional.of(new SqlStatementResult(
          queryId,
          statementState,
          taskResponse.getStatus().getCreatedTime(),
          signature.orElse(null),
          taskResponse.getStatus().getDuration(),
          withResults ? getResultSetInformation(
              queryId,
              msqControllerTask.getDataSource(),
              statementState,
              msqControllerTask.getQuerySpec().getDestination()
          ).orElse(null) : null,
          null,
          SqlStatementResourceHelper.getQueryStagesReport(taskReportPayload),
          SqlStatementResourceHelper.getQueryCounters(taskReportPayload),
          SqlStatementResourceHelper.getQueryWarningDetails(taskReportPayload)
      ));
    }
  }

  /**
   * Creates a new sqlQuery from the user submitted sqlQuery after performing required modifications.
   */
  private SqlQuery createModifiedSqlQuery(SqlQuery sqlQuery)
  {
    Map<String, Object> context = sqlQuery.getContext();
    if (context.containsKey(RESULT_FORMAT)) {
      throw InvalidInput.exception("Query context parameter [%s] is not allowed", RESULT_FORMAT);
    }
    Map<String, Object> modifiedContext = ImmutableMap.<String, Object>builder()
                                                      .putAll(context)
                                                      .put(RESULT_FORMAT, sqlQuery.getResultFormat().toString())
                                                      .build();
    return new SqlQuery(
        sqlQuery.getQuery(),
        sqlQuery.getResultFormat(),
        sqlQuery.includeHeader(),
        sqlQuery.includeTypesHeader(),
        sqlQuery.includeSqlTypesHeader(),
        modifiedContext,
        sqlQuery.getParameters()
    );
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
      MSQControllerTask msqControllerTask
  )
  {
    return SqlStatementResourceHelper.INSTANCE.getResultSequence(
        frame,
        finalStage.getFrameReader(),
        msqControllerTask.getQuerySpec().getColumnMappings(),
        new ResultsContext(msqControllerTask.getSqlTypeNames(), msqControllerTask.getSqlResultsContext()),
        jsonMapper
    );
  }

  private void resultPusher(
      String queryId,
      Optional<List<ColumnNameAndTypes>> signature,
      Closer closer,
      Optional<Yielder<Object[]>> results,
      CountingOutputStream os,
      ResultFormat resultFormat
  ) throws IOException
  {
    try {
      try (final ResultFormat.Writer writer = resultFormat.createFormatter(os, jsonMapper)) {
        Yielder<Object[]> yielder = results.get();
        List<ColumnNameAndTypes> rowSignature = signature.get();
        resultPusherInternal(writer, yielder, rowSignature);
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

  @Override
  protected String getDurableStorageErrorMsgTemplate()
  {
    return DURABLE_ERROR_TEMPLATE;
  }


}
