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

import org.apache.spark.util.StatCounter

trait SparklintTaskCounter {
  def outputMetrics: SparklintOutputCounter

  /**
    * DataReadMethod -> SparklintInputMetrics
    *
    * @return
    */
  def inputMetrics: SparklintInputCounter

  def shuffleReadMetrics: SparklintShuffleReadCounter

  def shuffleWriteMetrics: SparklintShuffleWriteCounter

  def diskBytesSpilled: StatCounter

  def memoryBytesSpilled: StatCounter

  def executorDeserializeTime: StatCounter

  def jvmGCTime: StatCounter

  def resultSerializationTime: StatCounter

  def resultSize: StatCounter

  def executorRunTime: StatCounter

  def merge(taskId: Long, metrics: SparklintTaskMetrics): SparklintTaskCounter
}
