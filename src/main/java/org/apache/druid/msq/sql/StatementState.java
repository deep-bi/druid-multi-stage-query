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

package org.apache.druid.msq.sql;

import org.apache.druid.msq.sql.resources.SqlStatementResource;
import org.apache.druid.sql.http.SqlQuery;

import javax.servlet.http.HttpServletRequest;

/**
 * Represents the status of the sql statements issues via
 * {@link SqlStatementResource#doPost(SqlQuery, HttpServletRequest)} or
 * {@link org.apache.druid.msq.nql.resources.NativeStatementResource#doPost(java.io.InputStream, HttpServletRequest)} and returned in
 * {@link org.apache.druid.msq.StatementResult}
 */
public enum StatementState
{
  // The statement is accepted but not yes assigned any worker. In MSQ engine, the statement is in ACCEPTED state
  // till the overlord assigns a TaskLocation to the controller task.
  ACCEPTED,
  // The statement is running.
  RUNNING,
  // The statement is successful.
  SUCCESS,
  // The statement failed.
  FAILED
}
