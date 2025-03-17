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

import org.apache.druid.frame.channel.FrameChannelSequence;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.exec.ClusterStatisticsMergeMode;
import org.apache.druid.msq.exec.Limits;
import org.apache.druid.msq.indexing.MSQTuningConfig;
import org.apache.druid.msq.input.stage.InputChannels;
import org.apache.druid.msq.input.stage.StageInputSpec;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.QueryDefinitionBuilder;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.kernel.StagePartition;
import org.apache.druid.msq.kernel.controller.ControllerQueryKernel;
import org.apache.druid.msq.querykit.QueryKitSpec;
import org.apache.druid.msq.querykit.results.QueryResultFrameProcessorFactory;

import java.io.IOException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ControllerUtil
{

  protected static final Logger log = new Logger(ControllerUtil.class);

  public static ClusterStatisticsMergeMode finalizeClusterStatisticsMergeMode(
      StageDefinition stageDef,
      ClusterStatisticsMergeMode initialMode
  )
  {
    ClusterStatisticsMergeMode mergeMode = initialMode;
    if (initialMode == ClusterStatisticsMergeMode.AUTO) {
      ClusterBy clusterBy = stageDef.getClusterBy();
      if (clusterBy.getBucketByCount() == 0) {
        // If there is no time clustering, there is no scope for sequential merge
        mergeMode = ClusterStatisticsMergeMode.PARALLEL;
      } else if (stageDef.getMaxWorkerCount() > Limits.MAX_WORKERS_FOR_PARALLEL_MERGE) {
        mergeMode = ClusterStatisticsMergeMode.SEQUENTIAL;
      } else {
        mergeMode = ClusterStatisticsMergeMode.PARALLEL;
      }
      log.info(
          "Stage [%d] AUTO mode: chose %s mode to merge key statistics",
          stageDef.getStageNumber(),
          mergeMode
      );
    }
    return mergeMode;
  }


  public static QueryDefinition queryDefinitionForDurableStorage(
      final QueryDefinition queryDef,
      final MSQTuningConfig tuningConfig,
      final QueryKitSpec queryKitSpec
  )
  {
    // attaching new query results stage if the final stage does sort during shuffle so that results are ordered.
    StageDefinition finalShuffleStageDef = queryDef.getFinalStageDefinition();
    if (finalShuffleStageDef.doesSortDuringShuffle()) {
      final QueryDefinitionBuilder builder = QueryDefinition.builder(queryKitSpec.getQueryId());
      builder.addAll(queryDef);
      builder.add(StageDefinition.builder(queryDef.getNextStageNumber())
                                 .inputs(new StageInputSpec(queryDef.getFinalStageDefinition().getStageNumber()))
                                 .maxWorkerCount(tuningConfig.getMaxNumWorkers())
                                 .signature(finalShuffleStageDef.getSignature())
                                 .shuffleSpec(null)
                                 .processorFactory(new QueryResultFrameProcessorFactory())
      );
      return builder.build();
    } else {
      return queryDef;
    }
  }

  public static Stream<FrameChannelSequence> createFrameChannelSequences(
      final ControllerQueryKernel queryKernel,
      final InputChannels inputChannels,
      final StageId finalStageId
  )
  {
    return StreamSupport.stream(queryKernel.getResultPartitionsForStage(finalStageId).spliterator(), false)
                        .map(readablePartition -> {
                          try {
                            return new FrameChannelSequence(
                                inputChannels.openChannel(
                                    new StagePartition(
                                        queryKernel.getStageDefinition(finalStageId).getId(),
                                        readablePartition.getPartitionNumber()
                                    )
                                )
                            );
                          }
                          catch (IOException e) {
                            throw new RuntimeException(e);
                          }
                        });
  }
}
