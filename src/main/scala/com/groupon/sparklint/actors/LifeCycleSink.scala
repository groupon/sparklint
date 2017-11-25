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
import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListenerApplicationStart}

/**
  * @author rxue
  * @since 6/2/17.
  */
object LifeCycleSink {
  val name: String = "lifeCycle"

  def props: Props = Props(new LifeCycleSink())

  trait Query extends SparklintLogProcessor.LogProcessorQuery

  case object GetLifeCycle extends Query

  case class LifeCycleResponse(started: Option[Long], finished: Option[Long]) {
    lazy val state: String = if (started.isEmpty) {
      "Unstarted"
    } else if (finished.isEmpty) {
      "Running"
    } else {
      "Finished"
    }

    lazy val duration: Option[Long] = {
      finished.map(f => f - started.get)
    }
  }

}

class LifeCycleSink extends Actor {

  import LifeCycleSink._

  private var started: Option[Long] = None
  private var ended: Option[Long] = None

  override def receive: Receive = {
    case start: SparkListenerApplicationStart =>
      started = Some(start.time)
    case end: SparkListenerApplicationEnd =>
      ended = Some(end.time)
    case GetLifeCycle =>
      sender() ! LifeCycleResponse(started, ended)
  }
}
