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

package org.apache.druid.msq.indexing.client;

import org.apache.druid.msq.exec.Controller;
import org.apache.druid.msq.indexing.IndexerResourcePermissionMapper;
import org.apache.druid.msq.rpc.ControllerResource;
import org.apache.druid.segment.realtime.ChatHandler;
import org.apache.druid.server.security.AuthorizerMapper;

public class ControllerChatHandler extends ControllerResource implements ChatHandler
{
  public ControllerChatHandler(Controller controller, String dataSource, AuthorizerMapper authorizerMapper)
  {
    super(controller, new IndexerResourcePermissionMapper(dataSource), authorizerMapper);
  }
}
