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

  case object StartReading

  case object StartInitializing

  case object ReadNextLine

  case object ReadTillEnd

  case object PauseReading

  case object ResumeReading

  def props(uuid: String, logs: Iterator[SparkListenerEvent], monitor: ActorRef): Props = Props(new SparklintAppLogReader(uuid, logs, monitor))

  sealed trait State

  case object Unstarted extends State

  case object Initializing extends State

  case object Started extends State

  case object Paused extends State

  case object Finished extends State

  sealed trait Data

  case class ProgressData(numRead: Int) extends Data

}

class SparklintAppLogReader(uuid: String, logs: Iterator[SparkListenerEvent], monitor: ActorRef)
  extends FSM[SparklintAppLogReader.State, SparklintAppLogReader.Data] {

  import SparklintAppLogReader._

  lazy val logProcessor: ActorRef = context.actorOf(SparklintLogProcessor.props(uuid), s"$uuid-${SparklintLogProcessor.name}")

  startWith(Unstarted, ProgressData(0))

  when(Unstarted) {
    case Event(StartInitializing, p: ProgressData) =>
      self ! StartInitializing
      goto(Initializing) using p
  }

  when(Initializing) {
    case Event(StartInitializing, p: ProgressData) =>
      if (logs.hasNext) {
        val logLine = logs.next()
        logProcessor ! logLine
        logLine match {
          case _: SparkListenerApplicationStart =>
            self ! ReadNextLine
            goto(Started) using p.copy(p.numRead + 1)
          case _ =>
            self ! StartInitializing
            stay() using p.copy(p.numRead + 1)
        }
      } else {
        goto(Finished) using p
      }
  }

  when(Started) {
    case Event(ReadTillEnd, p: ProgressData) =>
      if (logs.hasNext) {
        logProcessor ! logs.next()
        self ! ReadTillEnd
        stay() using p.copy(p.numRead + 1)
      } else {
        goto(Finished) using p
      }
    case Event(ReadNextLine, p: ProgressData) =>
      if (logs.hasNext) {
        logProcessor ! logs.next()
        stay() using p.copy(p.numRead + 1)
      } else {
        goto(Finished) using p
      }
    case Event(PauseReading, _) =>
      goto(Paused)
  }

  when(Paused) {
    case Event(ResumeReading, _) =>
      goto(Started)
  }

  when(Finished) {
    case _ =>
      stay()
  }

  whenUnhandled {
    case _ =>
      stay()
  }

}
