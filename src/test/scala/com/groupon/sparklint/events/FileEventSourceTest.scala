package com.groupon.sparklint.events

import java.io.File

import com.groupon.sparklint.TestUtils.resource
import com.groupon.sparklint.data.SparklintStateLike
import com.groupon.sparklint.data.compressed.CompressedState
import org.apache.spark.groupon.StringToSparkEvent
import org.apache.spark.scheduler.{SparkListenerEvent, SparkListenerTaskStart}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * @author swhitear 
  * @since 9/14/16.
  */
class FileEventSourceTest extends FlatSpec with Matchers {

  it should "throw up if the file does not exist" in {
    intercept[IllegalArgumentException] {
      FileEventSource(new File("wherefore/art/though/filey"), StubEventState())
    }
  }

  it should "handle an empty file just fine" in {
    val state = StubEventState()
    val source = FileEventSource(emptyFile, state)

    source.appId shouldEqual "file_event_log_empty_test"
    state.onEvents.isEmpty shouldEqual true
    state.unEvents.isEmpty shouldEqual true
    source.progress.atEnd shouldBe true
    source.progress.atStart shouldBe true
    source.progress.hasNext shouldBe false
    source.progress.hasPrevious shouldBe false

    intercept[IllegalArgumentException] {
      source.forwardEvents()
    }
    intercept[IllegalArgumentException] {
      source.rewindEvents()
    }
  }

  it should "set initial state if events in source" in {
    val state = new LosslessEventState()
    val source = FileEventSource(testFileWithState, state)

    source.version shouldEqual "1.5.2"
    source.appId shouldEqual "application_1462781278026_205691"
    source.appName shouldEqual "MyAppName"
    source.fullName shouldEqual "MyAppName (application_1462781278026_205691)"
    source.user shouldEqual "johndoe"
    source.host shouldEqual "10.22.81.222"
    source.port shouldEqual 44783
    source.maxMemory shouldEqual 2300455157l
    source.startTime shouldEqual 1466087746466l
    source.endTime shouldEqual 0
  }

  it should "allow two way iteration through file content" in {
    val state = StubEventState()
    val fileEvents = testEvents(testFileNoState)
    val source = FileEventSource(testFileNoState, state)

    source.appId shouldEqual "file_event_log_test_simple"
    state.onEvents.size shouldEqual 0
    state.unEvents.isEmpty shouldEqual true
    source.progress.atEnd shouldBe false
    source.progress.atStart shouldBe true
    source.progress.hasNext shouldBe true
    source.progress.hasPrevious shouldBe false

    source.forwardEvents(count = 5)
    state.onEvents.size shouldEqual 5
    getTaskId(state.onEvents(0)) shouldEqual getTaskId(fileEvents(0))
    getTaskId(state.onEvents(1)) shouldEqual getTaskId(fileEvents(1))
    getTaskId(state.onEvents(2)) shouldEqual getTaskId(fileEvents(2))
    getTaskId(state.onEvents(3)) shouldEqual getTaskId(fileEvents(3))
    getTaskId(state.onEvents(4)) shouldEqual getTaskId(fileEvents(4))
    state.unEvents.isEmpty shouldEqual true
    source.progress.atEnd shouldBe true
    source.progress.atStart shouldBe false
    source.progress.hasNext shouldBe false
    source.progress.hasPrevious shouldBe true

    source.rewindEvents(count = 5)
    state.onEvents.size shouldEqual 5
    getTaskId(state.onEvents(0)) shouldEqual getTaskId(fileEvents(0))
    getTaskId(state.onEvents(1)) shouldEqual getTaskId(fileEvents(1))
    getTaskId(state.onEvents(2)) shouldEqual getTaskId(fileEvents(2))
    getTaskId(state.onEvents(3)) shouldEqual getTaskId(fileEvents(3))
    getTaskId(state.onEvents(4)) shouldEqual getTaskId(fileEvents(4))
    state.unEvents.size shouldEqual 5
    getTaskId(state.unEvents(0)) shouldEqual getTaskId(fileEvents(4))
    getTaskId(state.unEvents(1)) shouldEqual getTaskId(fileEvents(3))
    getTaskId(state.unEvents(2)) shouldEqual getTaskId(fileEvents(2))
    getTaskId(state.unEvents(3)) shouldEqual getTaskId(fileEvents(1))
    getTaskId(state.unEvents(4)) shouldEqual getTaskId(fileEvents(0))
    source.progress.atEnd shouldBe false
    source.progress.atStart shouldBe true
    source.progress.hasNext shouldBe true
    source.progress.hasPrevious shouldBe false
  }

  private def testFileNoState: File = {
    new File(resource("file_event_log_test_simple"))
  }

  private def testFileWithState: File = {
    new File(resource("file_event_log_test_state_events"))
  }

  private def emptyFile: File = {
    new File(resource("file_event_log_empty_test"))
  }

  private def testEvents(testFile: File): Seq[SparkListenerEvent] = {
    Source.fromFile(testFile).getLines().map(StringToSparkEvent.apply).toSeq
  }

  private def getTaskId(event: SparkListenerEvent) = {
    event.asInstanceOf[SparkListenerTaskStart].taskInfo.taskId
  }
}

private case class StubEventState(onEvents: ArrayBuffer[SparkListenerEvent] = ArrayBuffer.empty,
                                  unEvents: ArrayBuffer[SparkListenerEvent] = ArrayBuffer.empty,
                                  state: SparklintStateLike = CompressedState.empty)
  extends EventStateLike {

  override def onEvent(event: SparkListenerEvent): Unit = onEvents += event

  override def unEvent(event: SparkListenerEvent): Unit = unEvents += event

  override def getState: SparklintStateLike = state
}
