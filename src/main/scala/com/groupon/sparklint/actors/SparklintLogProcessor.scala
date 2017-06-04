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
import org.apache.spark.groupon.SparkListenerLogStartShim
import org.apache.spark.scheduler._

/**
  * @author rxue
  * @since 6/2/17.
  */
object SparklintLogProcessor {
  val name: String = "logProcessor"

  def props(uuid: String): Props = Props(new SparklintLogProcessor(uuid))

}

class SparklintLogProcessor(uuid: String) extends Actor {
  lazy val version: ActorRef = context.actorOf(VersionSink.props, s"$uuid-${VersionSink.name}")
  lazy val lifeCycle: ActorRef = context.actorOf(LifeCycleSink.props, s"$uuid-${LifeCycleSink.name}")
  lazy val executors: ActorRef = context.actorOf(ExecutorSink.props, s"$uuid-${ExecutorSink.name}")
  lazy val taskInfo: ActorRef = context.actorOf(JobSink.props, s"$uuid-${JobSink.name}")

  private var lastMessageAt: Long = 0L

  override def receive: Receive = {
    case e: SparkListenerLogStartShim =>
      version.forward(e)

    case e: SparkListenerApplicationStart =>
      lastMessageAt = e.time
      lifeCycle.forward(e)
    case e: SparkListenerApplicationEnd =>
      lastMessageAt = e.time
      lifeCycle.forward(e)
      executors.forward(e)
      taskInfo.forward(e)

    case e: SparkListenerExecutorAdded =>
      lastMessageAt = e.time
      executors.forward(e)
      taskInfo.forward(e)
    case e: SparkListenerExecutorRemoved =>
      lastMessageAt = e.time
      executors.forward(e)
      taskInfo.forward(e)

    case e: SparkListenerJobStart =>
      lastMessageAt = e.time
      taskInfo.forward(e)
    case e: SparkListenerJobEnd =>
      lastMessageAt = e.time
      taskInfo.forward(e)
    case e: SparkListenerStageSubmitted =>
      for (submissionTime <- e.stageInfo.submissionTime) {
        lastMessageAt = submissionTime
      }
      taskInfo.forward(e)
    case e: SparkListenerStageCompleted =>
      for (completionTime <- e.stageInfo.completionTime) {
        lastMessageAt = completionTime
      }
      taskInfo.forward(e)
    case e: SparkListenerTaskStart =>
      lastMessageAt = e.taskInfo.launchTime
      taskInfo.forward(e)
    case e: SparkListenerTaskEnd =>
      lastMessageAt = e.taskInfo.finishTime
      taskInfo.forward(e)
  }
}
