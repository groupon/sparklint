package com.groupon.sparklint.events

import java.io.File

import com.groupon.sparklint.TestUtils.resource
import com.groupon.sparklint.common.Utils
import com.groupon.sparklint.data.SparklintStateLike
import com.groupon.sparklint.data.compressed.CompressedState
import org.apache.spark.groupon.StringToSparkEvent
import org.apache.spark.scheduler._
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

  it should "throw up if negative scroll count used" in {
    val state = new LosslessEventState()
    val source = FileEventSource(testFileWithState, state)

    intercept[IllegalArgumentException] {
      source.forwardEvents(count = -1)
    }
    intercept[IllegalArgumentException] {
      source.rewindEvents(count = -1)
    }
  }

  it should "handle an empty file just fine" in {
    val state = StubEventState()
    val source = FileEventSource(emptyFile, state)

    source.appId shouldEqual "file_event_log_empty_test"
    source.appName shouldEqual Utils.UNKNOWN_STRING
    source.nameOrId shouldEqual "file_event_log_empty_test"

    state.onEvents.isEmpty shouldEqual true
    state.unEvents.isEmpty shouldEqual true
    source.progress.hasNext shouldBe false
    source.progress.hasPrevious shouldBe false

    source.forwardEvents()
    state.onEvents.isEmpty shouldEqual true
    state.unEvents.isEmpty shouldEqual true
    source.rewindEvents()
    state.onEvents.isEmpty shouldEqual true
    state.unEvents.isEmpty shouldEqual true
  }

  it should "set initial state if events in source" in {
    val state = new LosslessEventState()
    val source = FileEventSource(testFileWithState, state)

    source.version shouldEqual "1.5.2"
    source.appId shouldEqual "application_1462781278026_205691"
    source.trimmedId shouldEqual "1462781278026_205691"
    source.appName shouldEqual "MyAppName"
    source.fullName shouldEqual "MyAppName (application_1462781278026_205691)"
    source.nameOrId shouldEqual "MyAppName"
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
    source.progress.hasNext shouldBe true
    source.progress.hasPrevious shouldBe false

    source.forwardEvents(count = 5)
    state.onEvents.size shouldEqual 5
    testIdsMatchForRange(state.onEvents, fileEvents, 0 to 4)
    state.unEvents.isEmpty shouldEqual true
    source.progress.hasNext shouldBe false
    source.progress.hasPrevious shouldBe true

    source.rewindEvents(count = 5)
    state.onEvents.size shouldEqual 5
    testIdsMatchForRange(state.onEvents, fileEvents, 0 to 4)
    state.unEvents.size shouldEqual 5
    testIdsMatchForRange(state.unEvents, fileEvents, 0 to 4, 4 to 0 by -1)
    source.progress.hasNext shouldBe true
    source.progress.hasPrevious shouldBe false
  }

  it should "allow two way task iteration through file content" in {
    val state = StubEventState()
    val fileEvents = testEvents(testFileWithNavEvents)
    val source = FileEventSource(testFileWithNavEvents, state)

    source.appId shouldEqual "file_event_log_test_nav_events"
    state.onEvents.size shouldEqual 0
    state.unEvents.isEmpty shouldEqual true
    source.progress.hasNext shouldBe true
    source.progress.hasPrevious shouldBe false

    source.forwardTasks()
    state.onEvents.size shouldEqual 4
    testIdsMatchForRange(state.onEvents, fileEvents, 0 to 3)
    state.unEvents.isEmpty shouldEqual true
    source.progress.hasNext shouldBe true
    source.progress.hasPrevious shouldBe true

    source.forwardTasks(count = 2)
    state.onEvents.size shouldEqual 14
    testIdsMatchForRange(state.onEvents, fileEvents, 0 to 13)
    state.unEvents.isEmpty shouldEqual true
    source.progress.hasNext shouldBe true
    source.progress.hasPrevious shouldBe true

    source.rewindTasks()
    state.onEvents.size shouldEqual 14
    testIdsMatchForRange(state.onEvents, fileEvents, 0 to 13)
    state.unEvents.size shouldEqual 2
    testIdsMatchForRange(state.unEvents, fileEvents, 0 to 1, 13 to 12 by -1)
    source.progress.hasNext shouldBe true
    source.progress.hasPrevious shouldBe true

    source.rewindTasks(count = 2)
    state.onEvents.size shouldEqual 14
    testIdsMatchForRange(state.onEvents, fileEvents, 0 to 13)
    state.unEvents.size shouldEqual 12
    testIdsMatchForRange(state.unEvents, fileEvents, 0 to 11, 13 to 3 by -1)
    source.progress.hasNext shouldBe true
    source.progress.hasPrevious shouldBe true
  }

  it should "allow two way stage iteration through file content" in {
    val state = StubEventState()
    val fileEvents = testEvents(testFileWithNavEvents)
    val source = FileEventSource(testFileWithNavEvents, state)

    source.appId shouldEqual "file_event_log_test_nav_events"
    state.onEvents.size shouldEqual 0
    state.unEvents.isEmpty shouldEqual true
    source.progress.hasNext shouldBe true
    source.progress.hasPrevious shouldBe false

    source.forwardStages()
    state.onEvents.size shouldEqual 5
    testIdsMatchForRange(state.onEvents, fileEvents, 0 to 4)
    state.unEvents.isEmpty shouldEqual true
    source.progress.hasNext shouldBe true
    source.progress.hasPrevious shouldBe true

    source.forwardStages(count = 2)
    state.onEvents.size shouldEqual 15
    testIdsMatchForRange(state.onEvents, fileEvents, 0 to 14)
    state.unEvents.isEmpty shouldEqual true
    source.progress.hasNext shouldBe true
    source.progress.hasPrevious shouldBe true

    source.rewindStages()
    state.onEvents.size shouldEqual 15
    testIdsMatchForRange(state.onEvents, fileEvents, 0 to 14)
    state.unEvents.size shouldEqual 4
    testIdsMatchForRange(state.unEvents, fileEvents, 0 to 3, 14 to 11 by -1)
    source.progress.hasNext shouldBe true
    source.progress.hasPrevious shouldBe true

    source.rewindStages(count = 2)
    state.onEvents.size shouldEqual 15
    testIdsMatchForRange(state.onEvents, fileEvents, 0 to 14)
    state.unEvents.size shouldEqual 14
    testIdsMatchForRange(state.unEvents, fileEvents, 0 to 13, 14 to 2 by -1)
    source.progress.hasNext shouldBe true
    source.progress.hasPrevious shouldBe true
  }

  it should "allow two way job iteration through file content" in {
    val state = StubEventState()
    val fileEvents = testEvents(testFileWithNavEvents)
    val source = FileEventSource(testFileWithNavEvents, state)

    source.appId shouldEqual "file_event_log_test_nav_events"
    state.onEvents.size shouldEqual 0
    state.unEvents.isEmpty shouldEqual true
    source.progress.hasNext shouldBe true
    source.progress.hasPrevious shouldBe false

    source.forwardJobs()
    state.onEvents.size shouldEqual 6
    testIdsMatchForRange(state.onEvents, fileEvents, 0 to 5)
    state.unEvents.isEmpty shouldEqual true
    source.progress.hasNext shouldBe true
    source.progress.hasPrevious shouldBe true

    source.forwardJobs()
    state.onEvents.size shouldEqual 16
    testIdsMatchForRange(state.onEvents, fileEvents, 0 to 15)
    state.unEvents.isEmpty shouldEqual true
    source.progress.hasNext shouldBe false
    source.progress.hasPrevious shouldBe true

    source.rewindJobs(count = 2)
    state.onEvents.size shouldEqual 16
    testIdsMatchForRange(state.onEvents, fileEvents, 0 to 15)
    state.unEvents.size shouldEqual 16
    testIdsMatchForRange(state.unEvents, fileEvents, 0 to 15, 15 to 0 by -1)
    source.progress.hasNext shouldBe true
    source.progress.hasPrevious shouldBe false
  }

  private def testFileNoState: File = {
    new File(resource("file_event_log_test_simple"))
  }

  private def testFileWithState: File = {
    new File(resource("file_event_log_test_state_events"))
  }

  private def testFileWithNavEvents: File = {
    new File(resource("file_event_log_test_nav_events"))
  }

  private def emptyFile: File = {
    new File(resource("file_event_log_empty_test"))
  }

  private def testEvents(testFile: File): Seq[SparkListenerEvent] = {
    Source.fromFile(testFile).getLines().map(StringToSparkEvent.apply).toSeq
  }

  private def testIdsMatchForRange(test: Seq[SparkListenerEvent], expected: Seq[SparkListenerEvent],
                                   seq: Seq[Int]): Unit = {
    testIdsMatchForRange(test, expected, seq, seq)
  }
  private def testIdsMatchForRange(test: Seq[SparkListenerEvent], expected: Seq[SparkListenerEvent],
                                   testSeq: Seq[Int], expectedSeq: Seq[Int]): Unit = {
    testSeq.zip(expectedSeq).foreach(pair => testIdsMatch(test(pair._1), expected(pair._2)))
  }

  private def testIdsMatch(test: SparkListenerEvent, expected: SparkListenerEvent) = (test, expected) match {
    case (tst: SparkListenerTaskStart, exp: SparkListenerTaskStart)           => tst.taskInfo.taskId shouldEqual exp.taskInfo.taskId
    case (tst: SparkListenerTaskEnd, exp: SparkListenerTaskEnd)               => tst.taskInfo.taskId shouldEqual exp.taskInfo.taskId
    case (tst: SparkListenerStageSubmitted, exp: SparkListenerStageSubmitted) => tst.stageInfo.stageId shouldEqual exp.stageInfo.stageId
    case (tst: SparkListenerStageCompleted, exp: SparkListenerStageCompleted) => tst.stageInfo.stageId shouldEqual exp.stageInfo.stageId
    case (tst: SparkListenerJobStart, exp: SparkListenerJobStart)             => tst.jobId shouldEqual exp.jobId
    case (tst: SparkListenerJobEnd, exp: SparkListenerJobEnd)                 => tst.jobId shouldEqual exp.jobId
    case _                                                                    => fail("Mismatched or unhandled event types")
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
