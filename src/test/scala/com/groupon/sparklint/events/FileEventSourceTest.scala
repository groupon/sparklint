package com.groupon.sparklint.events

import java.io.File

import com.groupon.sparklint.TestUtils.resource
import com.groupon.sparklint.common.Utils
import org.apache.spark.groupon.StringToSparkEvent
import org.apache.spark.scheduler._
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

import scala.io.Source

/**
  * @author swhitear 
  * @since 9/14/16.
  */
class FileEventSourceTest extends FlatSpec with Matchers with BeforeAndAfterEach {

  var state: StubEventState = _
  var progress: EventSourceProgress = _

  override protected def beforeEach(): Unit = {
    state = new StubEventState()
    progress = new EventSourceProgress()
  }

  it should "throw up if the file does not exist" in {
    intercept[IllegalArgumentException] {
      FileEventSource(new File("wherefore/art/though/filey"), progress, state)
    }
  }

  it should "throw up if negative scroll count used" in {
    val source = FileEventSource(testFileWithState, progress, state)

    intercept[IllegalArgumentException] {
      source.forwardEvents(count = -1)
    }
    intercept[IllegalArgumentException] {
      source.rewindEvents(count = -1)
    }
  }

  it should "handle an empty file just fine" in {
    val source = FileEventSource(emptyFile, progress, state)

    source.appId shouldEqual "file_event_log_empty_test"
    source.appName shouldEqual Utils.UNKNOWN_STRING
    source.nameOrId shouldEqual "file_event_log_empty_test"

    state.eventCount shouldEqual 0
    source.hasNext shouldBe false
    source.hasPrevious shouldBe false

    source.forwardEvents()
    state.eventCount shouldEqual 0
    source.hasNext shouldBe false
    source.hasPrevious shouldBe false

    source.rewindEvents()
    state.eventCount shouldEqual 0
    source.hasNext shouldBe false
    source.hasPrevious shouldBe false
  }

  it should "set initial state if events in source" in {
    val source = FileEventSource(testFileWithState, progress, state)

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

  it should "preprocess the source events" in {
    val fileEvents = testEvents(testFileNoState)
    val source = FileEventSource(testFileNoState, progress, state)

    source.appId shouldEqual "file_event_log_test_simple"
    state.eventCount shouldEqual 5
    state.preprocCount shouldEqual 5
    state.onCount shouldEqual 0
    state.unCount shouldEqual 0
    source.hasNext shouldBe true
    source.hasPrevious shouldBe false
    testIdsMatchForRange(state.preprocEvents, fileEvents, 0 to 4)
  }

  it should "allow two way iteration through file content" in {
    val fileEvents = testEvents(testFileNoState)
    val source = FileEventSource(testFileNoState, progress, state)

    source.appId shouldEqual "file_event_log_test_simple"
    state.eventCount shouldEqual 5
    state.preprocCount shouldEqual 5
    state.onCount shouldEqual 0
    state.unCount shouldEqual 0
    source.hasNext shouldBe true
    source.hasPrevious shouldBe false

    source.forwardEvents(count = 5)
    state.onCount shouldEqual 5
    testIdsMatchForRange(state.onEvents, fileEvents, 0 to 4)
    state.unCount shouldEqual 0
    source.hasNext shouldBe false
    source.hasPrevious shouldBe true

    source.rewindEvents(count = 5)
    state.onCount shouldEqual 5
    testIdsMatchForRange(state.onEvents, fileEvents, 0 to 4)
    state.unCount shouldEqual 5
    testIdsMatchForRange(state.unEvents, fileEvents, 0 to 4, 4 to 0 by -1)
    source.hasNext shouldBe true
    source.hasPrevious shouldBe false
  }

  it should "allow two way task iteration through file content" in {
    val fileEvents = testEvents(testFileWithNavEvents)
    val source = FileEventSource(testFileWithNavEvents, progress, state)

    source.appId shouldEqual "file_event_log_test_nav_events"
    state.eventCount shouldEqual 16
    state.preprocCount shouldEqual 16
    state.onCount shouldEqual 0
    state.unCount shouldEqual 0
    source.hasNext shouldBe true
    source.hasPrevious shouldBe false

    source.forwardTasks()
    state.onCount shouldEqual 4
    testIdsMatchForRange(state.onEvents, fileEvents, 0 to 3)
    state.unCount shouldEqual 0
    source.hasNext shouldBe true
    source.hasPrevious shouldBe true

    source.forwardTasks(count = 2)
    state.onCount shouldEqual 14
    testIdsMatchForRange(state.onEvents, fileEvents, 0 to 13)
    state.unCount shouldEqual 0
    source.hasNext shouldBe true
    source.hasPrevious shouldBe true

    source.rewindTasks()
    state.onCount shouldEqual 14
    testIdsMatchForRange(state.onEvents, fileEvents, 0 to 13)
    state.unCount shouldEqual 2
    testIdsMatchForRange(state.unEvents, fileEvents, 0 to 1, 13 to 12 by -1)
    source.hasNext shouldBe true
    source.hasPrevious shouldBe true

    source.rewindTasks(count = 2)
    state.onCount shouldEqual 14
    testIdsMatchForRange(state.onEvents, fileEvents, 0 to 13)
    state.unCount shouldEqual 12
    testIdsMatchForRange(state.unEvents, fileEvents, 0 to 11, 13 to 3 by -1)
    source.hasNext shouldBe true
    source.hasPrevious shouldBe true
  }

  it should "allow two way stage iteration through file content" in {
    val fileEvents = testEvents(testFileWithNavEvents)
    val source = FileEventSource(testFileWithNavEvents, progress, state)

    source.appId shouldEqual "file_event_log_test_nav_events"
    state.eventCount shouldEqual 16
    state.preprocCount shouldEqual 16
    state.onCount shouldEqual 0
    state.unCount shouldEqual 0
    source.hasNext shouldBe true
    source.hasPrevious shouldBe false

    source.forwardStages()
    state.onCount shouldEqual 5
    testIdsMatchForRange(state.onEvents, fileEvents, 0 to 4)
    state.unCount shouldEqual 0
    source.hasNext shouldBe true
    source.hasPrevious shouldBe true

    source.forwardStages(count = 2)
    state.onCount shouldEqual 15
    testIdsMatchForRange(state.onEvents, fileEvents, 0 to 14)
    state.unCount shouldEqual 0
    source.hasNext shouldBe true
    source.hasPrevious shouldBe true

    source.rewindStages()
    state.onCount shouldEqual 15
    testIdsMatchForRange(state.onEvents, fileEvents, 0 to 14)
    state.unCount shouldEqual 4
    testIdsMatchForRange(state.unEvents, fileEvents, 0 to 3, 14 to 11 by -1)
    source.hasNext shouldBe true
    source.hasPrevious shouldBe true

    source.rewindStages(count = 2)
    state.onCount shouldEqual 15
    testIdsMatchForRange(state.onEvents, fileEvents, 0 to 14)
    state.unCount shouldEqual 14
    testIdsMatchForRange(state.unEvents, fileEvents, 0 to 13, 14 to 2 by -1)
    source.hasNext shouldBe true
    source.hasPrevious shouldBe true
  }

  it should "allow two way job iteration through file content" in {
    val fileEvents = testEvents(testFileWithNavEvents)
    val source = FileEventSource(testFileWithNavEvents, progress, state)

    source.appId shouldEqual "file_event_log_test_nav_events"
    state.eventCount shouldEqual 16
    state.preprocCount shouldEqual 16
    state.onCount shouldEqual 0
    state.unCount shouldEqual 0
    source.hasNext shouldBe true
    source.hasPrevious shouldBe false

    source.forwardJobs()
    state.onCount shouldEqual 6
    testIdsMatchForRange(state.onEvents, fileEvents, 0 to 5)
    state.unCount shouldEqual 0
    source.hasNext shouldBe true
    source.hasPrevious shouldBe true

    source.forwardJobs()
    state.onCount shouldEqual 16
    testIdsMatchForRange(state.onEvents, fileEvents, 0 to 15)
    state.unCount shouldEqual 0
    source.hasNext shouldBe false
    source.hasPrevious shouldBe true

    source.rewindJobs(count = 2)
    state.onCount shouldEqual 16
    testIdsMatchForRange(state.onEvents, fileEvents, 0 to 15)
    state.unCount shouldEqual 16
    testIdsMatchForRange(state.unEvents, fileEvents, 0 to 15, 15 to 0 by -1)
    source.hasNext shouldBe true
    source.hasPrevious shouldBe false
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


