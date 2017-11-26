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

import akka.actor.{ActorRef, FSM, Props}
import org.apache.spark.scheduler.{SparkListenerApplicationStart, SparkListenerEvent}

/**
  * @author rxue
  * @since 6/2/17.
  */
object SparklintAppLogReader {

  case object StartInitializing

  case object ReadNextLine

  private case object ReadTillEnd

  case object PauseReading

  case object ResumeReading

  def props(id: String, logs: Iterator[SparkListenerEvent], storageOption: StorageOption): Props =
    Props(new SparklintAppLogReader(id, logs, storageOption))

  sealed trait State

  private[actors] case object Unstarted extends State

  private[actors] case object Initializing extends State

  private[actors] case object Started extends State

  private[actors] case object Paused extends State

  private[actors] case object Finished extends State

  case class ProgressData(numRead: Int) {
    def inc(count: Int = 1): ProgressData = copy(numRead + count)
  }

  case object GetReaderStatus

  case class GetReaderStatusResponse(state: String, progress: ProgressData, lastRead: Option[SparkListenerEvent],
                                     storageOption: StorageOption)
}

class SparklintAppLogReader(id: String, logs: Iterator[SparkListenerEvent], storageOption: StorageOption)
  extends FSM[SparklintAppLogReader.State, SparklintAppLogReader.ProgressData] {

  import SparklintAppLogReader._

  lazy val logProcessor: ActorRef = context.actorOf(SparklintLogProcessor.props(id, storageOption), SparklintLogProcessor.name)

  var lastRead: Option[SparkListenerEvent] = None

  startWith(Unstarted, ProgressData(0))

  when(Unstarted) {
    case Event(StartInitializing, p: ProgressData) =>
      self ! StartInitializing
      goto(Initializing) using p
  }

  when(Initializing) {
    case Event(StartInitializing, p: ProgressData) =>
      var readCount = p.numRead
      var successfullyInitialized = false
      while (logs.hasNext && !successfullyInitialized) {
        val logLine = logs.next()
        lastRead = Some(logLine)
        logProcessor ! logLine
        readCount += 1
        logLine match {
          case _: SparkListenerApplicationStart =>
            successfullyInitialized = true
          case _ =>
        }
      }
      val newProgress = p.copy(readCount)
      if (successfullyInitialized) {
        goto(Paused) using newProgress
      } else {
        goto(Finished) using newProgress
      }
    case Event(e@(ResumeReading | ReadTillEnd), _) =>
      self ! e
      stay()
  }

  when(Paused) {
    case Event(e@(ResumeReading | ReadNextLine), p) =>
      if (e == ResumeReading) {
        self ! ReadTillEnd
      } else {
        self ! ReadNextLine
      }
      goto(Started)
  }

  when(Started) {
    case Event(e@(ReadTillEnd | ReadNextLine), p) =>
      if (logs.hasNext) {
        val logLine = logs.next()
        lastRead = Some(logLine)
        logProcessor ! logLine
        val newProgress = p.inc()
        if (e == ReadTillEnd) {
          self ! ReadTillEnd
          stay() using newProgress
        } else {
          goto(Paused) using newProgress
        }
      } else {
        goto(Finished) using p
      }
    case Event(PauseReading, _) =>
      goto(Paused)
  }

  when(Finished) {
    PartialFunction.empty
  }

  whenUnhandled {
    case Event(GetReaderStatus, p) =>
      sender() ! GetReaderStatusResponse(stateName.toString, p, lastRead, storageOption)
      stay()
    case Event(query: SparklintLogProcessor.LogProcessorQuery, _) =>
      logProcessor.forward(query)
      stay()
    case _ =>
      stay()
  }

}
