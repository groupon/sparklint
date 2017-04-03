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
  * @since 12/4/16.
  */
trait SparklintStageMetrics {
  /**
    * (TaskLocality, TaskType) -> TaskMetrics, to provide metrics group by TaskLocality and TaskType
    *
    * @return
    */
  def metricsRepo: Map[(TaskLocality, Symbol), SparklintTaskCounter]

  def merge(taskId: Long, taskType: Symbol, locality: TaskLocality, metrics: SparklintTaskMetrics): SparklintStageMetrics
}
