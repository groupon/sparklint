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

package com.groupon.sparklint.events

import java.io.File

import com.groupon.sparklint.common.{TestUtils, Utils}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

/**
  * @author swhitear 
  * @since 9/14/16.
  */
class EventSourceMetaTest extends FlatSpec with Matchers with BeforeAndAfterEach {

  var meta: EventSourceMetaLike = _

  override protected def beforeEach(): Unit = {
    meta = new EventSourceMeta(EventSourceIdentifier("appId", Some("attemptId")), "appName")
  }

  it should "handle default as expected" in {
    meta.appIdentifier shouldEqual Utils.UNKNOWN_STRING
    meta.appName shouldEqual Utils.UNKNOWN_STRING
    meta.user shouldEqual Utils.UNKNOWN_STRING
    meta.version shouldEqual Utils.UNKNOWN_STRING
    meta.host shouldEqual Utils.UNKNOWN_STRING
    meta.port shouldEqual Utils.UNKNOWN_NUMBER
    meta.maxMemory shouldEqual Utils.UNKNOWN_NUMBER
    meta.startTime shouldEqual Utils.UNKNOWN_NUMBER
    meta.endTime shouldEqual Utils.UNKNOWN_NUMBER
  }

  it should "set state if events in source" in {
    val source = FileEventSource(testFileWithState)
    TestUtils.replay(source)

    meta.version shouldEqual "1.5.2"
    meta.appIdentifier shouldEqual "application_1462781278026_205691"
    meta.appName shouldEqual "MyAppName"
    meta.fullName shouldEqual "MyAppName (application_1462781278026_205691)"
    meta.user shouldEqual "johndoe"
    meta.host shouldEqual "10.22.81.222"
    meta.port shouldEqual 44783
    meta.maxMemory shouldEqual 2300455157l
    meta.startTime shouldEqual 1466087746466l
    meta.endTime shouldEqual 0
  }

  private def testFileWithState: File = {
    new File(TestUtils.resource("file_event_log_test_state_events"))
  }
}


