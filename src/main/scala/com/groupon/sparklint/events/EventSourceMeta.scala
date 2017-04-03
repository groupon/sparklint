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

package com.groupon.sparklint.events

import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListenerApplicationStart}

/**
  * @author rxue
  * @since 1.0.5
  */
case class EventSourceMeta(appId: Option[String],
                           attempt: Option[String],
                           appName: String,
                           sparkVersion: Option[String],
                           var startTime: Long = System.currentTimeMillis()) extends EventReceiverLike {
  lazy val fullAppId: String = s"${appId.getOrElse("no-appId")}${attempt.map(att => s"-$att").getOrElse("")}"
  var endTime: Option[Long] = None

  override protected def preprocAddApp(event: SparkListenerApplicationStart): Unit = {
    startTime = event.time
  }

  override protected def preprocEndApp(event: SparkListenerApplicationEnd): Unit = {
    endTime = Some(event.time)
  }
}
