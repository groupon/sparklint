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

import org.apache.spark.executor.TaskMetrics

/**
  * @author rxue
  * @since 12/4/16.
  */
object SparkVersionSpecificToSparklint {
  def sparklintTaskMetrics(m: TaskMetrics): SparklintTaskMetrics = {
    SparklintTaskMetrics(
      SparkToSparklint.sparklintOutputMetrics(m.outputMetrics),
      SparkToSparklint.sparklintInputMetrics(m.inputMetrics),
      SparkToSparklint.sparklintShuffleReadMetrics(m.shuffleReadMetrics),
      SparkToSparklint.sparklintShuffleWriteMetrics(m.shuffleWriteMetrics),
      m.diskBytesSpilled,
      m.memoryBytesSpilled,
      m.executorDeserializeTime,
      m.jvmGCTime,
      m.resultSerializationTime,
      m.resultSize,
      m.executorRunTime
    )
  }
}
