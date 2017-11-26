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

  trait LogProcessorQuery

}

class SparklintLogProcessor(id: String) extends Actor {
  lazy val version: ActorRef = context.actorOf(VersionSink.props, VersionSink.name)
  lazy val lifeCycle: ActorRef = context.actorOf(LifeCycleSink.props, LifeCycleSink.name)
  lazy val executors: ActorRef = context.actorOf(ExecutorSink.props, ExecutorSink.name)
  lazy val jobInfo: ActorRef = context.actorOf(JobSink.props, JobSink.name)

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
      jobInfo.forward(e)

    case e: SparkListenerExecutorAdded =>
      lastMessageAt = e.time
      executors.forward(e)
      jobInfo.forward(e)
    case e: SparkListenerExecutorRemoved =>
      lastMessageAt = e.time
      executors.forward(e)
      jobInfo.forward(e)

    case e: SparkListenerJobStart =>
      lastMessageAt = e.time
      jobInfo.forward(e)
    case e: SparkListenerJobEnd =>
      lastMessageAt = e.time
      jobInfo.forward(e)
    case e: SparkListenerStageSubmitted =>
      for (submissionTime <- e.stageInfo.submissionTime) {
        lastMessageAt = submissionTime
      }
      jobInfo.forward(e)
    case e: SparkListenerStageCompleted =>
      for (completionTime <- e.stageInfo.completionTime) {
        lastMessageAt = completionTime
      }
      jobInfo.forward(e)
    case e: SparkListenerTaskStart =>
      lastMessageAt = e.taskInfo.launchTime
      jobInfo.forward(e)
    case e: SparkListenerTaskEnd =>
      lastMessageAt = e.taskInfo.finishTime
      jobInfo.forward(e)

    case query: VersionSink.Query =>
      version.forward(query)
    case query: ExecutorSink.Query =>
      executors.forward(query)
    case query: LifeCycleSink.Query =>
      lifeCycle.forward(query)
    case query: JobSink.Query =>
      jobInfo.forward(query)
  }
}
