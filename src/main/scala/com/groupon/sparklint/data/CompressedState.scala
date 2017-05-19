/*
 * Copyright 2016 Groupon, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.groupon.sparklint.data

import org.apache.spark.scheduler.TaskLocality.TaskLocality

/**
  * @author rxue
  * @since 8/22/16.
  */
case class CompressedState(coreUsageByLocality: Map[TaskLocality, CompressedMetricsSink],
                           coreUsageByPool: Map[Symbol, CompressedMetricsSink],
                           executorInfo: Map[String, SparklintExecutorInfo],
                           stageMetrics: Map[SparklintStageIdentifier, CompressedStageMetrics],
                           stageIdLookup: Map[Int, SparklintStageIdentifier],
                           runningTasks: Map[Long, SparklintTaskInfo],
                           firstTaskAt: Option[Long],
                           applicationEndedAt: Option[Long],
                           lastUpdatedAt: Long) extends SparklintStateLike

object CompressedState {
  def empty: CompressedState = new CompressedState(Map.empty, Map.empty, Map.empty, Map.empty, Map.empty, Map.empty, None, None, 0L)
}
