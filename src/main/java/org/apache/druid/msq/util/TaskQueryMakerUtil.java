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

package org.apache.druid.msq.util;

import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.msq.indexing.destination.DurableStorageMSQDestination;
import org.apache.druid.msq.indexing.destination.MSQDestination;
import org.apache.druid.msq.indexing.destination.MSQSelectDestination;
import org.apache.druid.msq.indexing.destination.TaskReportMSQDestination;
import org.apache.druid.query.QueryContext;
import org.apache.druid.sql.calcite.parser.DruidSqlReplace;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class TaskQueryMakerUtil
{

  public static List<Interval> replaceTimeChunks(final QueryContext queryContext)
  {
    return Optional.ofNullable(queryContext.get(DruidSqlReplace.SQL_REPLACE_TIME_CHUNKS))
                   .map(
                       s -> {
                         if (s instanceof String && "all".equals(StringUtils.toLowerCase((String) s))) {
                           return Intervals.ONLY_ETERNITY;
                         } else {
                           final String[] parts = ((String) s).split("\\s*,\\s*");
                           final List<Interval> intervals = new ArrayList<>();

                           for (final String part : parts) {
                             intervals.add(Intervals.of(part));
                           }

                           return intervals;
                         }
                       }
                   )
                   .orElse(null);
  }

  public static MSQDestination selectDestination(final QueryContext queryContext)
  {
    final MSQSelectDestination msqSelectDestination = MultiStageQueryContext.getSelectDestination(queryContext);
    if (msqSelectDestination.equals(MSQSelectDestination.TASKREPORT)) {
      return TaskReportMSQDestination.instance();
    } else if (msqSelectDestination.equals(MSQSelectDestination.DURABLESTORAGE)) {
      return DurableStorageMSQDestination.instance();
    } else {
      throw InvalidInput.exception(
          "Unsupported select destination [%s] provided in the query context. MSQ can currently write the select results to "
          + "[%s]",
          msqSelectDestination.getName(),
          Arrays.stream(MSQSelectDestination.values())
                .map(MSQSelectDestination::getName)
                .collect(Collectors.joining(","))
      );
    }
  }
}
