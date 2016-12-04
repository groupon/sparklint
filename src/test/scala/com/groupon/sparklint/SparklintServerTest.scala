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
package com.groupon.sparklint

import java.io.File

import com.groupon.sparklint.common.{ScheduledTask, SchedulerLike, SparklintConfig}
import com.groupon.sparklint.common.TestUtils._
import com.groupon.sparklint.events.FileEventSourceManager
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

import scala.collection.mutable.ArrayBuffer

/**
  * @author swhitear 
  * @since 8/18/16.
  */
class SparklintServerTest extends FlatSpec with BeforeAndAfterEach with Matchers {

  private val TEMP_FILE_CONTENT =
    """{"Event":"SparkListenerApplicationStart","App Name":"MyAppName","App ID":"temp_addded_in_test","Timestamp":1466087746466,"User":"johndoe"}|"""

  private var server            : SparklintServer        = _
  private var eventSourceManager: FileEventSourceManager = _
  private var dirname           : String                 = _
  private var tempFile          : File                   = _
  private var scheduler         : StubScheduler          = _
  private var config            : SparklintConfig        = _

  override protected def beforeEach(): Unit = {
    eventSourceManager = new FileEventSourceManager()
    scheduler = new StubScheduler()
    dirname = resource("directory_source")
    tempFile = resetTempFile(dirname)
    config = SparklintConfig(exitOnError = false)
    server = new SparklintServer(eventSourceManager, scheduler, config)
  }

  override protected def afterEach(): Unit = {
    server.shutdownUI()
  }

  it should "load expected buffer from a file when configured" in {
    val filename = resource("spark_event_log_example")
    val args = Seq("-f", filename).toArray
    config.parseCliArgs(args)
    server.buildEventSources()
    server.startUI()

    eventSourceManager.eventSourceDetails.size shouldEqual 1
    scheduler.scheduledTasks.isEmpty shouldBe true

    val es = eventSourceManager.getScrollingSource("spark_event_log_example")
    es.eventSourceId shouldEqual "spark_event_log_example"
    es.hasNext shouldEqual true
    es.hasPrevious shouldEqual false
  }

  it should "load expected buffer from a file and replay when configured" in {
    val filename = resource("spark_event_log_example")
    val args = Seq("-f", filename, "-r").toArray
    config.parseCliArgs(args)
    server.buildEventSources()
    server.startUI()

    eventSourceManager.eventSourceDetails.size shouldEqual 1
    scheduler.scheduledTasks.isEmpty shouldBe true

    val es = eventSourceManager.getScrollingSource("spark_event_log_example")
    es.eventSourceId shouldEqual "spark_event_log_example"
    es.hasNext shouldEqual false
    es.hasPrevious shouldEqual true
  }


  it should "load expected buffer from a directory when configured" in {
    val dirname = resource("directory_source")
    val args = Seq("-d", dirname).toArray
    config.parseCliArgs(args)
    server.buildEventSources()
    server.startUI()

    eventSourceManager.eventSourceDetails.size shouldEqual 0
    scheduler.scheduledTasks.size shouldEqual 1

    // fire the timed event to load from directory
    scheduler.scheduledTasks.head.run()
    eventSourceManager.eventSourceDetails.size shouldEqual 2

    var es = eventSourceManager.getScrollingSource("event_log_0")
    es.eventSourceId shouldEqual "event_log_0"
    es.hasNext shouldEqual true
    es.hasPrevious shouldEqual false

    es = eventSourceManager.getScrollingSource("event_log_1")
    es.eventSourceId shouldEqual "event_log_1"
    es.hasNext shouldEqual true
    es.hasPrevious shouldEqual false
  }

  it should "load expected buffer from a directory and replay when configured" in {
    val args = Seq("-d", dirname, "-r").toArray
    config.parseCliArgs(args)
    server.buildEventSources()
    server.startUI()

    eventSourceManager.eventSourceDetails.size shouldEqual 0
    scheduler.scheduledTasks.size shouldEqual 1

    // fire the timed event to load from directory
    scheduler.scheduledTasks.head.run()
    eventSourceManager.eventSourceDetails.size shouldEqual 2

    var es = eventSourceManager.getScrollingSource("event_log_0")
    es.eventSourceId shouldEqual "event_log_0"
    es.hasNext shouldEqual false
    es.hasPrevious shouldEqual true

    es = eventSourceManager.getScrollingSource("event_log_1")
    es.eventSourceId shouldEqual "event_log_1"
    es.hasNext shouldEqual false
    es.hasPrevious shouldEqual true
  }

  it should "refresh with the latest new files when task fired" in {
    val args = Seq("-d", dirname).toArray
    config.parseCliArgs(args)
    server.buildEventSources()
    server.startUI()

    eventSourceManager.eventSourceDetails.size shouldEqual 0
    scheduler.scheduledTasks.size shouldEqual 1

    // fire the timed event to load from directory
    scheduler.scheduledTasks.head.run()
    eventSourceManager.eventSourceDetails.size shouldEqual 2

    eventSourceManager.eventSourceDetails.count(_.eventSourceId == "event_log_0") shouldEqual 1
    eventSourceManager.eventSourceDetails.count(_.eventSourceId == "event_log_1") shouldEqual 1

    // add a new file to the directory
    addInTempFile(tempFile)

    eventSourceManager.eventSourceDetails.size shouldEqual 2
    scheduler.scheduledTasks.head.run()
    eventSourceManager.eventSourceDetails.size shouldEqual 3

    eventSourceManager.eventSourceDetails.count(_.eventSourceId == "event_log_0") shouldEqual 1
    eventSourceManager.eventSourceDetails.count(_.eventSourceId == "event_log_1") shouldEqual 1
    eventSourceManager.eventSourceDetails.count(_.eventSourceId == "temp_addded_in_test") shouldEqual 1

    // cleanup again
    cleanupTempFile(tempFile)
  }

  it should "new files should auto play if configured" in {
    val args = Seq("-d", dirname, "-r").toArray
    config.parseCliArgs(args)
    server.buildEventSources()
    server.startUI()

    // fire the timed event to load from directory
    scheduler.scheduledTasks.head.run()
    eventSourceManager.eventSourceDetails.size shouldEqual 2

    // add a new file to the directory
    addInTempFile(tempFile)

    scheduler.scheduledTasks.head.run()
    eventSourceManager.eventSourceDetails.size shouldEqual 3

    val es = eventSourceManager.getScrollingSource("temp_addded_in_test")
    es.eventSourceId shouldEqual "temp_addded_in_test"
    es.hasNext shouldEqual false
    es.hasPrevious shouldEqual true

    // cleanup again
    cleanupTempFile(tempFile)
  }

  private def resetTempFile(dirname: String): File = {
    // will add this new file, so delete if exists
    val newFile: File = new File(dirname, "temp_addded_in_test")
    if (newFile.exists()) newFile.delete() shouldBe true
    newFile
  }

  private def addInTempFile(file: File, content: String = TEMP_FILE_CONTENT): File = {
    val pw = new java.io.PrintWriter(file)
    try pw.write(content) finally pw.close()
    file
  }

  private def cleanupTempFile(file: File) = {
    if (file.exists()) file.delete() shouldBe true
  }
}

//class StubEventSourceManager(override val eventSourceDetails: ArrayBuffer[EventSourceLike] = ArrayBuffer[EventSourceLike]())
//  extends FileEventSourceManager

class StubScheduler(val scheduledTasks: ArrayBuffer[ScheduledTask[_]] = ArrayBuffer[ScheduledTask[_]]())
  extends SchedulerLike {

  override def scheduleTask[T](task: ScheduledTask[T]): Unit = scheduledTasks += task

  override def cancelAll(): Unit = ???
}
