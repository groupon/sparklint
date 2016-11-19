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

  var stateManager   : StubEventStateManager      = _
  var progressTracker: EventSourceProgressTracker = _

  override protected def beforeEach(): Unit = {
    stateManager = new StubEventStateManager()
    progressTracker = new EventSourceProgressTracker()
  }

  it should "throw up if the file does not exist" in {
    intercept[IllegalArgumentException] {
      FileEventSource(new File("wherefore/art/though/filey"), progressTracker, stateManager)
    }
  }

  it should "throw up if negative scroll count used" in {
    val source = FileEventSource(testFileWithState, progressTracker, stateManager)

    intercept[IllegalArgumentException] {
      source.forwardEvents(count = -1)
    }
    intercept[IllegalArgumentException] {
      source.rewindEvents(count = -1)
    }
  }

  it should "handle an empty file just fine" in {
    val source = FileEventSource(emptyFile, progressTracker, stateManager)

    source.appId shouldEqual "file_event_log_empty_test"
    source.appName shouldEqual Utils.UNKNOWN_STRING
    source.nameOrId shouldEqual "file_event_log_empty_test"

    stateManager.eventCount shouldEqual 0
    source.hasNext shouldBe false
    source.hasPrevious shouldBe false

    source.forwardEvents()
    stateManager.eventCount shouldEqual 0
    source.hasNext shouldBe false
    source.hasPrevious shouldBe false

    source.rewindEvents()
    stateManager.eventCount shouldEqual 0
    source.hasNext shouldBe false
    source.hasPrevious shouldBe false
  }

  it should "set initial state if events in source" in {
    val source = FileEventSource(testFileWithState, progressTracker, stateManager)

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
    val source = FileEventSource(testFileNoState, progressTracker, stateManager)

    source.appId shouldEqual "file_event_log_test_simple"
    stateManager.eventCount shouldEqual 5
    stateManager.preprocCount shouldEqual 5
    stateManager.onCount shouldEqual 0
    stateManager.unCount shouldEqual 0
    source.hasNext shouldBe true
    source.hasPrevious shouldBe false
    testIdsMatchForRange(stateManager.preprocEvents, fileEvents, 0 to 4)
  }

  it should "allow two way iteration through file content" in {
    val fileEvents = testEvents(testFileNoState)
    val source = FileEventSource(testFileNoState, progressTracker, stateManager)

    source.appId shouldEqual "file_event_log_test_simple"
    stateManager.eventCount shouldEqual 5
    stateManager.preprocCount shouldEqual 5
    stateManager.onCount shouldEqual 0
    stateManager.unCount shouldEqual 0
    source.hasNext shouldBe true
    source.hasPrevious shouldBe false

    source.forwardEvents(count = 5)
    stateManager.onCount shouldEqual 5
    testIdsMatchForRange(stateManager.onEvents, fileEvents, 0 to 4)
    stateManager.unCount shouldEqual 0
    source.hasNext shouldBe false
    source.hasPrevious shouldBe true

    source.rewindEvents(count = 5)
    stateManager.onCount shouldEqual 5
    testIdsMatchForRange(stateManager.onEvents, fileEvents, 0 to 4)
    stateManager.unCount shouldEqual 5
    testIdsMatchForRange(stateManager.unEvents, fileEvents, 0 to 4, 4 to 0 by -1)
    source.hasNext shouldBe true
    source.hasPrevious shouldBe false
  }

  it should "allow two way task iteration through file content" in {
    val fileEvents = testEvents(testFileWithNavEvents)
    val source = FileEventSource(testFileWithNavEvents, progressTracker, stateManager)

    source.appId shouldEqual "file_event_log_test_nav_events"
    stateManager.eventCount shouldEqual 16
    stateManager.preprocCount shouldEqual 16
    stateManager.onCount shouldEqual 0
    stateManager.unCount shouldEqual 0
    source.hasNext shouldBe true
    source.hasPrevious shouldBe false

    source.forwardTasks()
    stateManager.onCount shouldEqual 4
    testIdsMatchForRange(stateManager.onEvents, fileEvents, 0 to 3)
    stateManager.unCount shouldEqual 0
    source.hasNext shouldBe true
    source.hasPrevious shouldBe true

    source.forwardTasks(count = 2)
    stateManager.onCount shouldEqual 14
    testIdsMatchForRange(stateManager.onEvents, fileEvents, 0 to 13)
    stateManager.unCount shouldEqual 0
    source.hasNext shouldBe true
    source.hasPrevious shouldBe true

    source.rewindTasks()
    stateManager.onCount shouldEqual 14
    testIdsMatchForRange(stateManager.onEvents, fileEvents, 0 to 13)
    stateManager.unCount shouldEqual 2
    testIdsMatchForRange(stateManager.unEvents, fileEvents, 0 to 1, 13 to 12 by -1)
    source.hasNext shouldBe true
    source.hasPrevious shouldBe true

    source.rewindTasks(count = 2)
    stateManager.onCount shouldEqual 14
    testIdsMatchForRange(stateManager.onEvents, fileEvents, 0 to 13)
    stateManager.unCount shouldEqual 12
    testIdsMatchForRange(stateManager.unEvents, fileEvents, 0 to 11, 13 to 3 by -1)
    source.hasNext shouldBe true
    source.hasPrevious shouldBe true
  }

  it should "allow two way stage iteration through file content" in {
    val fileEvents = testEvents(testFileWithNavEvents)
    val source = FileEventSource(testFileWithNavEvents, progressTracker, stateManager)

    source.appId shouldEqual "file_event_log_test_nav_events"
    stateManager.eventCount shouldEqual 16
    stateManager.preprocCount shouldEqual 16
    stateManager.onCount shouldEqual 0
    stateManager.unCount shouldEqual 0
    source.hasNext shouldBe true
    source.hasPrevious shouldBe false

    source.forwardStages()
    stateManager.onCount shouldEqual 5
    testIdsMatchForRange(stateManager.onEvents, fileEvents, 0 to 4)
    stateManager.unCount shouldEqual 0
    source.hasNext shouldBe true
    source.hasPrevious shouldBe true

    source.forwardStages(count = 2)
    stateManager.onCount shouldEqual 15
    testIdsMatchForRange(stateManager.onEvents, fileEvents, 0 to 14)
    stateManager.unCount shouldEqual 0
    source.hasNext shouldBe true
    source.hasPrevious shouldBe true

    source.rewindStages()
    stateManager.onCount shouldEqual 15
    testIdsMatchForRange(stateManager.onEvents, fileEvents, 0 to 14)
    stateManager.unCount shouldEqual 4
    testIdsMatchForRange(stateManager.unEvents, fileEvents, 0 to 3, 14 to 11 by -1)
    source.hasNext shouldBe true
    source.hasPrevious shouldBe true

    source.rewindStages(count = 2)
    stateManager.onCount shouldEqual 15
    testIdsMatchForRange(stateManager.onEvents, fileEvents, 0 to 14)
    stateManager.unCount shouldEqual 14
    testIdsMatchForRange(stateManager.unEvents, fileEvents, 0 to 13, 14 to 2 by -1)
    source.hasNext shouldBe true
    source.hasPrevious shouldBe true
  }

  it should "allow two way job iteration through file content" in {
    val fileEvents = testEvents(testFileWithNavEvents)
    val source = FileEventSource(testFileWithNavEvents, progressTracker, stateManager)

    source.appId shouldEqual "file_event_log_test_nav_events"
    stateManager.eventCount shouldEqual 16
    stateManager.preprocCount shouldEqual 16
    stateManager.onCount shouldEqual 0
    stateManager.unCount shouldEqual 0
    source.hasNext shouldBe true
    source.hasPrevious shouldBe false

    source.forwardJobs()
    stateManager.onCount shouldEqual 6
    testIdsMatchForRange(stateManager.onEvents, fileEvents, 0 to 5)
    stateManager.unCount shouldEqual 0
    source.hasNext shouldBe true
    source.hasPrevious shouldBe true

    source.forwardJobs()
    stateManager.onCount shouldEqual 16
    testIdsMatchForRange(stateManager.onEvents, fileEvents, 0 to 15)
    stateManager.unCount shouldEqual 0
    source.hasNext shouldBe false
    source.hasPrevious shouldBe true

    source.rewindJobs(count = 2)
    stateManager.onCount shouldEqual 16
    testIdsMatchForRange(stateManager.onEvents, fileEvents, 0 to 15)
    stateManager.unCount shouldEqual 16
    testIdsMatchForRange(stateManager.unEvents, fileEvents, 0 to 15, 15 to 0 by -1)
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


