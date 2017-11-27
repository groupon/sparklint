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

/**
  * @author rxue
  * @since 11/26/17.
  */
sealed trait StorageOption {
  def emptyCounter: MetricsCounter
}

object StorageOption {
  case object Lossless extends StorageOption {
    override def emptyCounter: MetricsCounter = new LosslessMetricsCounter
  }

  case class FixedCapacity(capacityMillis: Long, pruneFrequency: Long) extends StorageOption {
    require(capacityMillis > 0)
    require(pruneFrequency > 0)
    override def emptyCounter: MetricsCounter = new FixedCapacityMetricsCounter(capacityMillis, pruneFrequency)
  }
}
