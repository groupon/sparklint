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
import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListenerExecutorAdded, SparkListenerExecutorRemoved}

import scala.collection.mutable

/**
  * @author rxue
  * @since 6/2/17.
  */
object ExecutorSink {
  val name: String = "executors"

  def props: Props = Props(new ExecutorSink())

  trait Query extends SparklintLogProcessor.LogProcessorQuery

  case object GetLiveExecutors extends Query

  case object GetDeadExecutors extends Query

  case object GetAllExecutors extends Query

  case class ExecutorsResponse(executors: Map[String, ExecutorSummary]) {
    def cores: Int = {
      executors.values.filter(_.alive).map(_.cores).sum
    }
  }

  case class ExecutorSummary(added: Long, cores: Int, removed: Option[Long] = None) {
    def alive: Boolean = {
      removed.isEmpty
    }
  }

}

class ExecutorSink extends Actor {

  import ExecutorSink._

  private val liveExecutors = mutable.Map[String, ExecutorSummary]()
  private val deadExecutors = mutable.Map[String, ExecutorSummary]()

  override def receive: Receive = {
    case e: SparkListenerExecutorAdded =>
      liveExecutors(e.executorId) = ExecutorSummary(e.time, e.executorInfo.totalCores)
    case e: SparkListenerExecutorRemoved =>
      for (executorSummary <- liveExecutors.remove(e.executorId)) {
        deadExecutors(e.executorId) = executorSummary.copy(removed = Some(e.time))
      }
    case e: SparkListenerApplicationEnd =>
      for ((executorId, executorSummary) <- liveExecutors) {
        deadExecutors(executorId) = executorSummary.copy(removed = Some(e.time))
      }
      liveExecutors.clear()

    case GetLiveExecutors =>
      sender() ! ExecutorsResponse(liveExecutors.toMap)
    case GetDeadExecutors =>
      sender() ! ExecutorsResponse(deadExecutors.toMap)
    case GetAllExecutors =>
      sender() ! ExecutorsResponse(liveExecutors.toMap ++ deadExecutors.toMap)
  }
}
