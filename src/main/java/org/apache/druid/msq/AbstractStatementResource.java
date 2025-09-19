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

package org.apache.druid.msq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.ErrorResponse;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.channel.FrameChannelSequence;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.common.task.AbstractTask;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.indexing.IsMSQTask;
import org.apache.druid.msq.indexing.destination.DurableStorageMSQDestination;
import org.apache.druid.msq.indexing.destination.MSQDestination;
import org.apache.druid.msq.indexing.destination.MSQSelectDestination;
import org.apache.druid.msq.indexing.destination.TaskReportMSQDestination;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.shuffle.input.DurableStorageInputChannelFactory;
import org.apache.druid.msq.sql.MSQTaskQueryMaker;
import org.apache.druid.msq.sql.StatementState;
import org.apache.druid.msq.sql.entity.PageInformation;
import org.apache.druid.msq.sql.entity.ResultSetInformation;
import org.apache.druid.msq.util.AbstractResourceHelper;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.msq.util.SqlStatementResourceHelper;
import org.apache.druid.query.ExecutionMode;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.rpc.HttpResponseException;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.storage.NilStorageConnector;
import org.apache.druid.storage.StorageConnector;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.validation.constraints.NotNull;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public abstract class AbstractStatementResource<ResultType extends StatementResult, TaskType extends AbstractTask & IsMSQTask>
{
  public static final String CONTENT_DISPOSITION_RESPONSE_HEADER = "Content-Disposition";
  protected static final Pattern FILENAME_PATTERN = Pattern.compile("^[^/:*?><\\\\\"|\0\n\r]*$");
  private static final Logger log = new Logger(AbstractStatementResource.class);
  protected final ObjectMapper jsonMapper;
  protected final OverlordClient overlordClient;
  protected final StorageConnector storageConnector;

  public AbstractStatementResource(
      ObjectMapper jsonMapper,
      OverlordClient overlordClient,
      StorageConnector storageConnector
  )
  {
    this.jsonMapper = jsonMapper;
    this.overlordClient = overlordClient;
    this.storageConnector = storageConnector;
  }

  protected static void throwIfQueryIsNotSuccessful(String queryId, TaskStatusPlus statusPlus)
  {
    StatementState statementState = AbstractResourceHelper.getSqlStatementState(statusPlus);
    if (statementState == StatementState.RUNNING || statementState == StatementState.ACCEPTED) {
      throw DruidException.forPersona(DruidException.Persona.USER)
                          .ofCategory(DruidException.Category.INVALID_INPUT)
                          .build(
                              "Query[%s] is currently in [%s] state. Please wait for it to complete.",
                              queryId,
                              statementState
                          );
    } else if (statementState == StatementState.FAILED) {
      throw DruidException.forPersona(DruidException.Persona.USER)
                          .ofCategory(DruidException.Category.INVALID_INPUT)
                          .build(
                              "Query[%s] failed. Check the status api for more details.",
                              queryId
                          );
    } else {
      // do nothing
    }
  }

  protected Response buildTaskResponse(Sequence<Object[]> sequence, AuthenticationResult authenticationResult)
  {
    List<Object[]> rows = sequence.toList();
    int numRows = rows.size();
    if (numRows != 1) {
      throw new RE("Expected a single row but got [%d] rows. Please check broker logs for more information.", numRows);
    }
    Object[] firstRow = rows.get(0);
    if (firstRow == null || firstRow.length != 1) {
      throw new RE(
          "Expected a single column but got [%s] columns. Please check broker logs for more information.",
          firstRow == null ? 0 : firstRow.length
      );
    }
    String taskId = String.valueOf(firstRow[0]);

    Optional<ResultType> statementResult = getStatementStatus(
        taskId,
        authenticationResult,
        true,
        Action.READ,
        false
    );

    if (statementResult.isPresent()) {
      return Response.status(Response.Status.OK).entity(statementResult.get()).build();
    } else {
      return buildNonOkResponse(
          DruidException.forPersona(DruidException.Persona.DEVELOPER)
                        .ofCategory(DruidException.Category.DEFENSIVE).build(
                            "Unable to find associated task for query id [%s]. Contact cluster admin to check overlord logs for [%s]",
                            taskId,
                            taskId
                        )
      );
    }
  }

  /**
   * This method contacts the overlord for the controller task and checks if the requested user has the
   * necessary permissions. A user has the necessary permissions if one of the following criteria is satisfied:
   * 1. The user is the one who submitted the query
   * 2. The user belongs to a role containing the READ or WRITE permissions over the STATE resource. For endpoints like GET,
   * the user should have READ permission for the STATE resource, while for endpoints like DELETE, the user should
   * have WRITE permission for the STATE resource. (Note: POST API does not need to check the state permissions since
   * the currentUser always equal to the queryUser)
   */
  protected TaskType getMSQControllerTaskAndCheckPermission(
      String queryId,
      AuthenticationResult authenticationResult,
      Action forAction,
      AuthorizerMapper authorizerMapper
  ) throws ForbiddenException
  {

    TaskType msqControllerTask = getTaskEntity(queryId);
    String queryUser = String.valueOf(msqControllerTask.getQuerySpec()
                                                       .getContext()
                                                       .get(MSQTaskQueryMaker.USER_KEY));

    String currentUser = authenticationResult.getIdentity();

    if (currentUser != null && currentUser.equals(queryUser)) {
      return msqControllerTask;
    }

    AuthorizationResult authResult = AuthorizationUtils.authorizeAllResourceActions(
        authenticationResult,
        Collections.singletonList(new ResourceAction(Resource.STATE_RESOURCE, forAction)),
        authorizerMapper
    );

    if (!authResult.allowAccessWithNoRestriction()) {
      throw new ForbiddenException(StringUtils.format(
          "The current user[%s] cannot view query id[%s] since the query is owned by another user",
          currentUser,
          queryId
      ));
    }

    return msqControllerTask;
  }

  @SuppressWarnings("ReassignedVariable")
  protected Optional<ResultSetInformation> getResultSetInformation(
      String queryId,
      String dataSource,
      StatementState statementState,
      MSQDestination msqDestination
  )
  {
    if (statementState == StatementState.SUCCESS) {
      MSQTaskReportPayload msqTaskReportPayload =
          AbstractResourceHelper.getPayload(contactOverlord(
              overlordClient.taskReportAsMap(queryId),
              queryId
          ));
      Optional<List<PageInformation>> pageList = SqlStatementResourceHelper.populatePageList(
          msqTaskReportPayload,
          msqDestination
      );

      // getting the total number of rows, size from page information.
      Long rows = null;
      Long size = null;
      if (pageList.isPresent()) {
        rows = 0L;
        size = 0L;
        for (PageInformation pageInformation : pageList.get()) {
          rows += pageInformation.getNumRows() != null ? pageInformation.getNumRows() : 0L;
          size += pageInformation.getSizeInBytes() != null ? pageInformation.getSizeInBytes() : 0L;
        }
      }

      boolean isSelectQuery = msqDestination instanceof TaskReportMSQDestination
                              || msqDestination instanceof DurableStorageMSQDestination;

      List<Object[]> results = null;
      if (isSelectQuery) {
        results = new ArrayList<>();
        if (msqTaskReportPayload.getResults() != null) {
          results = msqTaskReportPayload.getResults().getResults();
        }
      }

      return Optional.of(
          new ResultSetInformation(
              rows,
              size,
              null,
              dataSource,
              results,
              isSelectQuery ? pageList.orElse(null) : null
          )
      );
    } else {
      return Optional.empty();
    }
  }

  protected Optional<Yielder<Object[]>> getResultYielder(
      String queryId,
      Long page,
      TaskType msqControllerTask,
      Closer closer
  )
  {
    final Optional<Yielder<Object[]>> results;

    if (msqControllerTask.getQuerySpec().getDestination() instanceof TaskReportMSQDestination) {
      // Results from task report are only present as one page.
      if (page != null && page > 0) {
        throw InvalidInput.exception(
            "Page number [%d] is out of the range of results", page
        );
      }

      MSQTaskReportPayload msqTaskReportPayload = SqlStatementResourceHelper.getPayload(
          contactOverlord(overlordClient.taskReportAsMap(queryId), queryId)
      );

      if (msqTaskReportPayload.getResults().getResults() == null) {
        results = Optional.empty();
      } else {
        results = Optional.of(Yielders.each(Sequences.simple(msqTaskReportPayload.getResults().getResults())));
      }

    } else if (msqControllerTask.getQuerySpec().getDestination() instanceof DurableStorageMSQDestination) {

      MSQTaskReportPayload msqTaskReportPayload = SqlStatementResourceHelper.getPayload(
          contactOverlord(overlordClient.taskReportAsMap(queryId), queryId)
      );

      List<PageInformation> pages =
          SqlStatementResourceHelper.populatePageList(
              msqTaskReportPayload,
              msqControllerTask.getQuerySpec().getDestination()
          ).orElse(null);

      if (pages == null || pages.isEmpty()) {
        return Optional.empty();
      }

      final StageDefinition finalStage = Objects.requireNonNull(SqlStatementResourceHelper.getFinalStage(
          msqTaskReportPayload)).getStageDefinition();

      // get all results
      final Long selectedPageId;
      if (page != null) {
        selectedPageId = getPageInformationForPageId(pages, page).getId();
      } else {
        selectedPageId = null;
      }
      checkForDurableStorageConnectorImpl();
      final DurableStorageInputChannelFactory standardImplementation = DurableStorageInputChannelFactory.createStandardImplementation(
          msqControllerTask.getId(),
          storageConnector,
          closer,
          true
      );
      results = Optional.of(Yielders.each(
          Sequences.concat(pages.stream()
                                .filter(pageInformation -> selectedPageId == null
                                                           || selectedPageId.equals(pageInformation.getId()))
                                .map(pageInformation -> {
                                  try {
                                    if (pageInformation.getWorker() == null || pageInformation.getPartition() == null) {
                                      throw DruidException.defensive(
                                          "Worker or partition number is null for page id [%d]",
                                          pageInformation.getId()
                                      );
                                    }
                                    return new FrameChannelSequence(standardImplementation.openChannel(
                                        finalStage.getId(),
                                        pageInformation.getWorker(),
                                        pageInformation.getPartition()
                                    ));
                                  }
                                  catch (Exception e) {
                                    throw new RuntimeException(e);
                                  }
                                })
                                .collect(Collectors.toList()))
                   .flatMap(frame -> getResultSequence(finalStage, frame, msqControllerTask)
                   )
                   .withBaggage(closer)));

    } else {
      throw DruidException.forPersona(DruidException.Persona.DEVELOPER)
                          .ofCategory(DruidException.Category.UNCATEGORIZED)
                          .build(
                              "MSQ select destination[%s] not supported. Please reach out to druid slack community for more help.",
                              msqControllerTask.getQuerySpec().getDestination().toString()
                          );
    }
    return results;
  }

  protected void checkForDurableStorageConnectorImpl()
  {
    if (storageConnector instanceof NilStorageConnector) {
      throw DruidException.forPersona(DruidException.Persona.USER)
                          .ofCategory(DruidException.Category.INVALID_INPUT)
                          .build(
                              StringUtils.format(
                                  getDurableStorageErrorMsgTemplate(),
                                  MSQSelectDestination.DURABLESTORAGE.getName(),
                                  MultiStageQueryContext.CTX_SELECT_DESTINATION,
                                  NodeRole.BROKER.getJsonName()
                              )
                          );
    }
  }

  private PageInformation getPageInformationForPageId(List<PageInformation> pages, Long pageId)
  {
    for (PageInformation pageInfo : pages) {
      if (pageInfo.getId() == pageId) {
        return pageInfo;
      }
    }
    throw InvalidInput.exception("Invalid page id [%d] passed.", pageId);
  }


  protected <T> T contactOverlord(final ListenableFuture<T> future, String queryId)
  {
    try {
      return FutureUtils.getUnchecked(future, true);
    }
    catch (RuntimeException e) {
      if (e.getCause() instanceof HttpResponseException) {
        HttpResponseException httpResponseException = (HttpResponseException) e.getCause();
        if (httpResponseException.getResponse() != null && httpResponseException.getResponse().getResponse().getStatus()
                                                                                .equals(HttpResponseStatus.NOT_FOUND)) {
          log.info(httpResponseException, "Query details not found for queryId [%s]", queryId);
          // since we get a 404, we mark the request as a NotFound. This code path is generally triggered when user passes a `queryId` which is not found in the overlord.
          throw queryNotFoundException(queryId);
        }
      }
      throw DruidException.forPersona(DruidException.Persona.DEVELOPER)
                          .ofCategory(DruidException.Category.UNCATEGORIZED)
                          .build("Unable to contact overlord " + e.getMessage());
    }
  }


  protected void contextChecks(QueryContext queryContext)
  {
    ExecutionMode executionMode = queryContext.getEnum(QueryContexts.CTX_EXECUTION_MODE, ExecutionMode.class, null);

    if (executionMode == null) {
      throw InvalidInput.exception(
          "Execution mode is not provided to the sql statement api. "
          + "Please set [%s] to [%s] in the query context",
          QueryContexts.CTX_EXECUTION_MODE,
          ExecutionMode.ASYNC
      );
    }

    if (!ExecutionMode.ASYNC.equals(executionMode)) {
      throw InvalidInput.exception(
          "The sql statement api currently does not support the provided execution mode [%s]. "
          + "Please set [%s] to [%s] in the query context",
          executionMode,
          QueryContexts.CTX_EXECUTION_MODE,
          ExecutionMode.ASYNC
      );
    }

    MSQSelectDestination selectDestination = MultiStageQueryContext.getSelectDestination(queryContext);
    if (MSQSelectDestination.DURABLESTORAGE.equals(selectDestination)) {
      checkForDurableStorageConnectorImpl();
    }
  }

  protected static Response.ResponseBuilder addContentDisposition(
          Response.ResponseBuilder responseBuilder,
          String contentDisposition
  )
  {
    if (contentDisposition != null) {
      responseBuilder.header(CONTENT_DISPOSITION_RESPONSE_HEADER, contentDisposition);
    }
    return responseBuilder;
  }

  /**
   * Validates that a filename is valid. Filenames are considered to be valid if it is:
   * <ul>
   *   <li>Not empty.</li>
   *   <li>Not longer than 255 characters.</li>
   *   <li>Does not contain the characters `/`, `\`, `:`, `*`, `?`, `"`, `<`, `>`, `|`, `\0`, `\n`, or `\r`.</li>
   * </ul>
   */
  @VisibleForTesting
  public static String validateFilename(@NotNull String filename)
  {
    if (filename.isEmpty()) {
      throw InvalidInput.exception("Filename cannot be empty.");
    }

    if (filename.length() > 255) {
      throw InvalidInput.exception("Filename cannot be longer than 255 characters.");
    }

    if (!FILENAME_PATTERN.matcher(filename).matches()) {
      throw InvalidInput.exception("Filename contains invalid characters. (/, \\, :, *, ?, \", <, >, |, \0, \n, or \r)");
    }
    return filename;
  }

  protected abstract Sequence<Object[]> getResultSequence(
      StageDefinition finalStage,
      Frame frame,
      TaskType msqControllerTask
  );

  protected abstract TaskType getTaskEntity(String queryId);


  protected Response buildNonOkResponse(DruidException exception)
  {
    return Response
        .status(exception.getStatusCode())
        .entity(new ErrorResponse(exception))
        .build();
  }

  protected abstract Optional<ResultType> getStatementStatus(
      String queryId,
      AuthenticationResult authenticationResult,
      boolean withResults,
      Action forAction,
      boolean detail
  ) throws DruidException;

  protected abstract DruidException queryNotFoundException(String queryId);

  protected abstract String getDurableStorageErrorMsgTemplate();
}
