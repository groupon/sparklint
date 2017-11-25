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

import java.util.UUID

import akka.actor.ActorRef
import akka.actor.FSM.{CurrentState, SubscribeTransitionCallBack, Transition, UnsubscribeTransitionCallBack}
import akka.testkit.{DefaultTimeout, TestKitBase}
import org.apache.spark.groupon.StringToSparkEvent

import scala.io.Source

/**
  * @author rxue
  * @since 6/4/17.
  */
trait FileBasedSparkLogTest {
  this: TestKitBase =>
  lazy val uuid: String = UUID.randomUUID().toString
  def fileName: String = "spark_event_log_example"

  import SparklintAppLogReader._

  protected def initializeSampleLog(): ActorRef = {
    val file = Source.fromFile(getClass.getClassLoader.getResource(fileName).getPath)
      .getLines().flatMap(StringToSparkEvent.apply)
    val reader: ActorRef = system.actorOf(SparklintAppLogReader.props(uuid, file))
    reader ! SubscribeTransitionCallBack(testActor)
    reader ! SparklintAppLogReader.StartInitializing
    reader
  }


  protected def readSampleLog(): ActorRef = {
    val file = Source.fromFile(getClass.getClassLoader.getResource(fileName).getPath)
      .getLines().flatMap(StringToSparkEvent.apply)
    val reader: ActorRef = system.actorOf(SparklintAppLogReader.props(uuid, file))
    reader ! SubscribeTransitionCallBack(testActor)
    reader ! StartInitializing
    expectMsg(CurrentState(reader, Unstarted))
    expectMsg(Transition(reader, Unstarted, Initializing))
    expectMsg(Transition(reader, Initializing, Started))
    reader ! ReadTillEnd
    expectMsg(Transition(reader, Started, Finished))
    reader ! UnsubscribeTransitionCallBack(testActor)
    reader
  }
}
