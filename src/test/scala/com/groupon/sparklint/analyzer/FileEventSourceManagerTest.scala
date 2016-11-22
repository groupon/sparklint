/*
 Copyright 2016 Groupon, Inc.
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
 http://www.apache.org/licenses/LICENSE-2.0
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/
package com.groupon.sparklint.analyzer

import org.scalatest.{FlatSpec, Matchers}

/**
  * @author swhitear
  * @since 9/13/16.
  */
class FileEventSourceManagerTest extends FlatSpec with Matchers {

  //  it should "initialize empty if no initial sources supplied" in {
  //    val manager = new FileEventSourceManager()
  //
  //    manager.sourceCount shouldEqual 0
  //    manager.eventSourceDetails.isEmpty shouldBe true
  //  }
  //
  //  it should "initialize with start state if initial sources supplied" in {
  //    val evDetail = stubEventSource("test_app_id")
  //    val manager = new FileEventSourceManager(evDetail)
  //
  //    manager.sourceCount shouldEqual 1
  //    manager.eventSourceDetails.head shouldBe evDetail
  //    manager.containsAppId("test_app_id") shouldBe true
  //    manager.getSourceDetail("test_app_id") shouldBe evDetail
  //  }
  //
  //  it should "add the extra event sources as expected and remain in order" in {
  //    val evDetail1 = stubEventSource("test_app_id_1")
  //    val evDetail2 = stubEventSource("test_app_id_2")
  //    val manager = new FileEventSourceManager(evDetail2)
  //    manager.addEventSource(evDetail1)
  //
  //    manager.sourceCount shouldEqual 2
  //    manager.eventSourceDetails.toSet shouldBe Set(evDetail1, evDetail2)
  //    manager.containsAppId("test_app_id_1") shouldBe true
  //    manager.containsAppId("test_app_id_2") shouldBe true
  //    manager.getSourceDetail("test_app_id_1") shouldBe evDetail1
  //    manager.getSourceDetail("test_app_id_2") shouldBe evDetail2
  //  }
  //
  //  it should "throw up when invalid appId specified for indexer" in {
  //    val evDetail = stubEventSource("test_app_id")
  //    val manager = new EventSourceManager(evDetail)
  //
  //    intercept[NoSuchElementException] {
  //      manager.getSourceDetail("invalid_app_id")
  //    }
  //  }
}
