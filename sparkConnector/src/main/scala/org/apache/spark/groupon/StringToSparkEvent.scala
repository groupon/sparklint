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

package org.apache.spark.groupon

import org.apache.spark.scheduler.{SparkListenerEvent, SparkListenerLogStart}
import org.apache.spark.util.JsonProtocol
import org.json4s.jackson.JsonMethods.parse

/**
  * A helper class. This is under org.apache.spark to utilize the private function JsonProtocal.sparkEventFromJson
  * so we can replay a spark event log from file system.
  *
  * @author rxue
  * @since 6/16/16.
  */
object StringToSparkEvent {

  def apply(line: String): SparkListenerEvent = shimIfNeeded(JsonProtocol.sparkEventFromJson(parse(line)))

  def as[T <: SparkListenerEvent](line: String): T = JsonProtocol.sparkEventFromJson(parse(line)).asInstanceOf[T]

  private def shimIfNeeded(event: SparkListenerEvent) = event match {
    case event: SparkListenerLogStart => new SparkListenerLogStartShim(event)
    case default => default
  }
}
