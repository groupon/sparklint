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

import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.testkit.{DefaultTimeout, TestKitBase}
import org.scalatest.{FeatureSpec, Matchers}

/**
  * @author rxue
  * @since 6/4/17.
  */
class LifeCycleSinkTest extends FeatureSpec
  with ScalatestRouteTest
  with TestKitBase
  with DefaultTimeout
  with FileBasedSparkLogTest
  with Matchers {

  import LifeCycleSink._

  feature("get version") {
    scenario("normally") {
      val reader = readSampleLog()
      val logProcessorPath = reader.path / s"$uuid-${SparklintLogProcessor.name}"
      val lifeCycleSink = system.actorSelection(logProcessorPath / s"$uuid-$name")
      lifeCycleSink ! GetLifeCycle(testActor)
      expectMsg(LifeCycleResponse(Some(1466087746466L), Some(1466088058982L)))
    }
  }
}