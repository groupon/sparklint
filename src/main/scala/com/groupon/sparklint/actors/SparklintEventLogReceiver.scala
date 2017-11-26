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

package com.groupon.sparklint.actors

import akka.actor.{Actor, ActorRef, Props}
import com.groupon.sparklint.actors.SparklintAppLogReader.{GetReaderStatus, GetReaderStatusResponse, ProgressData}
import org.apache.spark.scheduler.SparkListenerEvent

/**
  * @author rxue
  * @since 11/25/17.
  */
object SparklintEventLogReceiver {
  def props(id: String, storageOption: StorageOption): Props = Props(new SparklintEventLogReceiver(id, storageOption))
}

class SparklintEventLogReceiver(id: String, storageOption: StorageOption) extends Actor {

  lazy val logProcessor: ActorRef = context.actorOf(SparklintLogProcessor.props(id, storageOption), SparklintLogProcessor.name)
  var progressData: ProgressData = ProgressData(0)
  var lastRead: Option[SparkListenerEvent] = None

  override def receive: PartialFunction[Any, Unit] = {
    case logLine: SparkListenerEvent =>
      lastRead = Some(logLine)
      logProcessor ! logLine
      progressData = progressData.inc()
    case GetReaderStatus =>
      sender() ! GetReaderStatusResponse("Running", progressData, lastRead, storageOption)
    case query: SparklintLogProcessor.LogProcessorQuery =>
      logProcessor.forward(query)
  }
}
