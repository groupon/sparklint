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

  case class GetLiveExecutors(replyTo: ActorRef)

  case class GetDeadExecutors(replyTo: ActorRef)

  case class GetAllExecutors(replyTo: ActorRef)

  case class ExecutorsResponse(executors: Map[String, ExecutorSummary])

  case class ExecutorSummary(added: Long, cores: Int, removed: Option[Long] = None)

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
        liveExecutors.remove(executorId)
        deadExecutors(executorId) = executorSummary.copy(removed = Some(e.time))
      }

    case GetLiveExecutors(replyTo) =>
      replyTo ! ExecutorsResponse(liveExecutors.toMap)
    case GetDeadExecutors(replyTo) =>
      replyTo ! ExecutorsResponse(deadExecutors.toMap)
    case GetAllExecutors(replyTo) =>
      replyTo ! ExecutorsResponse(liveExecutors.toMap ++ deadExecutors.toMap)
  }
}
