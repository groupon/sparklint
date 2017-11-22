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

import akka.actor.ActorSystem
import akka.testkit.{DefaultTimeout, ImplicitSender, TestKit}
import org.scalatest.{FeatureSpecLike, Matchers}

/**
  * @author rxue
  * @since 6/4/17.
  */
class ExecutorSinkTest extends TestKit(ActorSystem("MySpec"))
  with FeatureSpecLike
  with ImplicitSender
  with DefaultTimeout
  with FileBasedSparkLogTest
  with Matchers {

  import ExecutorSink._

  feature("get version") {
    scenario("normally") {
      val reader = readSampleLog()
      val logProcessorPath = reader.path / s"$uuid-${SparklintLogProcessor.name}"
      val executorsSink = system.actorSelection(logProcessorPath / s"$uuid-$name")
      executorsSink ! GetAllExecutors
      expectMsg(ExecutorsResponse(Map(
        "2" -> ExecutorSummary(1466087808972L, 2, Some(1466088058982L)),
        "1" -> ExecutorSummary(1466087811580L, 2, Some(1466088058982L)),
        "3" -> ExecutorSummary(1466088056872L, 2, Some(1466088058982L)))))
    }
  }
}
