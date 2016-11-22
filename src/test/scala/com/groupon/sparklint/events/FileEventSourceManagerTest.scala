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
package com.groupon.sparklint.events

import org.scalatest.{FlatSpec, Matchers}

/**
  * @author swhitear
  * @since 9/13/16.
  */
class FileEventSourceManagerTest extends FlatSpec with Matchers {

    it should "initialize empty if no initial sources supplied" in {
      val manager = new FileEventSourceManager()

      manager.sourceCount shouldEqual 0
      manager.eventSourceDetails.isEmpty shouldBe true
    }

    it should "initialize with start state if initial sources supplied" in {
      val evDetail = constructDetails("test_app_id")
      val manager = new FileEventSourceManager
      manager.addEventSourceAndDetail(evDetail)

      manager.sourceCount shouldEqual 1
      manager.eventSourceDetails.head shouldBe evDetail.detail
      manager.containsEventSourceId("test_app_id") shouldBe true
      manager.getSourceDetail("test_app_id") shouldBe evDetail.detail
    }

    it should "add the extra event sources as expected and remain in order" in {
      val evDetail1 = constructDetails("test_app_id_1")
      val evDetail2 = constructDetails("test_app_id_2")
      val manager = new FileEventSourceManager
      manager.addEventSourceAndDetail(evDetail1)
      manager.addEventSourceAndDetail(evDetail2)

      manager.sourceCount shouldEqual 2
      manager.eventSourceDetails.toSet shouldBe Set(evDetail1.detail, evDetail2.detail)
      manager.containsEventSourceId("test_app_id_1") shouldBe true
      manager.containsEventSourceId("test_app_id_2") shouldBe true
      manager.getSourceDetail("test_app_id_1") shouldBe evDetail1.detail
      manager.getSourceDetail("test_app_id_2") shouldBe evDetail2.detail
    }

    it should "throw up when invalid appId specified for indexer" in {
      val evDetail = constructDetails("test_app_id")
      val manager = new EventSourceManager
      manager.addEventSourceAndDetail(evDetail)

      intercept[NoSuchElementException] {
        manager.getSourceDetail("invalid_app_id")
      }
    }

  private def constructDetails(eventSourceCtor: String): SourceAndDetail = {
    val es = StubEventSource(eventSourceCtor, Seq.empty)
    val detail = EventSourceDetail(eventSourceCtor, new EventSourceMeta(),
      new EventProgressTracker(), new StubEventStateManager())
    SourceAndDetail(es, detail)
  }

}
