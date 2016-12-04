package com.groupon.sparklint.events

import java.io.File

import com.groupon.sparklint.common.{TestUtils, Utils}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

/**
  * @author swhitear 
  * @since 9/14/16.
  */
class EventSourceMetaTest extends FlatSpec with Matchers with BeforeAndAfterEach {

  var meta: EventSourceMeta = _

  override protected def beforeEach(): Unit = {
    meta = new EventSourceMeta()
  }

  it should "handle default as expected" in {
    meta.appId shouldEqual Utils.UNKNOWN_STRING
    meta.appName shouldEqual Utils.UNKNOWN_STRING
    meta.nameOrId shouldEqual Utils.UNKNOWN_STRING
    meta.user shouldEqual Utils.UNKNOWN_STRING
    meta.version shouldEqual Utils.UNKNOWN_STRING
    meta.host shouldEqual Utils.UNKNOWN_STRING
    meta.port shouldEqual Utils.UNKNOWN_NUMBER
    meta.maxMemory shouldEqual Utils.UNKNOWN_NUMBER
    meta.startTime shouldEqual Utils.UNKNOWN_NUMBER
    meta.endTime shouldEqual Utils.UNKNOWN_NUMBER
  }

  it should "set state if events in source" in {
    val source = FileEventSource(testFileWithState, Seq(meta))
    TestUtils.replay(source)

    meta.version shouldEqual "1.5.2"
    meta.appId shouldEqual "application_1462781278026_205691"
    meta.trimmedId shouldEqual "1462781278026_205691"
    meta.appName shouldEqual "MyAppName"
    meta.fullName shouldEqual "MyAppName (application_1462781278026_205691)"
    meta.nameOrId shouldEqual "MyAppName"
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


