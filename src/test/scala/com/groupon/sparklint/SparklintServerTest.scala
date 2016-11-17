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

import com.groupon.sparklint.TestUtils.resource
import com.groupon.sparklint.common.{ScheduledTask, SchedulerLike, SparklintConfig}
import com.groupon.sparklint.events.{EventSourceDetail, EventSourceLike, EventSourceManagerLike, FreeScrollEventSource}
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
  private var eventSourceManager: StubEventSourceManager = _
  private var dirname           : String                 = _
  private var tempFile          : File                   = _
  private var scheduler         : StubScheduler          = _
  private var config            : SparklintConfig        = _

  override protected def beforeEach(): Unit = {
    eventSourceManager = StubEventSourceManager()
    scheduler = StubScheduler()
    dirname = resource("directory_source")
    tempFile = resetTempFile(dirname)
    config = SparklintConfig(exitOnError = false)
    server = new SparklintServer(eventSourceManager, scheduler, config)
  }

  override protected def afterEach(): Unit = {
    server.shutdown()
  }

  it should "load expected buffer from a file when configured" in {
    val filename = resource("spark_event_log_example")
    val args = Seq("-f", filename).toArray
    config.parseCliArgs(args)
    server.buildEventSources()
    server.run()

    eventSourceManager.eventSource.size shouldEqual 1
    scheduler.scheduledTasks.isEmpty shouldBe true

    val es = eventSourceManager.eventSource.head
    es.source.appId shouldEqual "application_1462781278026_205691"
    es.progress.eventProgress.hasNext shouldEqual true
    es.progress.eventProgress.hasPrevious shouldEqual false
  }

  it should "load expected buffer from a file and replay when configured" in {
    val filename = resource("spark_event_log_example")
    val args = Seq("-f", filename, "-r").toArray
    config.parseCliArgs(args)
    server.buildEventSources()
    server.run()

    eventSourceManager.eventSource.size shouldEqual 1
    scheduler.scheduledTasks.isEmpty shouldBe true

    val es = eventSourceManager.eventSource.head
    es.source.appId shouldEqual "application_1462781278026_205691"
    es.progress.eventProgress.hasNext shouldEqual false
    es.progress.eventProgress.hasPrevious shouldEqual true
  }


  it should "load expected buffer from a directory when configured" in {
    val dirname = resource("directory_source")
    val args = Seq("-d", dirname).toArray
    config.parseCliArgs(args)
    server.buildEventSources()
    server.run()

    eventSourceManager.eventSource.size shouldEqual 0
    scheduler.scheduledTasks.size shouldEqual 1

    // fire the timed event to load from directory
    scheduler.scheduledTasks.head.run()
    eventSourceManager.eventSource.size shouldEqual 2

    var es = eventSourceManager.eventSource.filter(_.source.appId == "application_1462781278026_205691").head
    es.source.appId shouldEqual "application_1462781278026_205691"
    es.progress.eventProgress.hasNext shouldEqual true
    es.progress.eventProgress.hasPrevious shouldEqual false

    es = eventSourceManager.eventSource.filter(_.source.appId == "application_1472176676028_116806").head
    es.source.appId shouldEqual "application_1472176676028_116806"
    es.progress.eventProgress.hasNext shouldEqual true
    es.progress.eventProgress.hasPrevious shouldEqual false
  }

  it should "load expected buffer from a directory and replay when configured" in {
    val args = Seq("-d", dirname, "-r").toArray
    config.parseCliArgs(args)
    server.buildEventSources()
    server.run()

    eventSourceManager.eventSource.size shouldEqual 0
    scheduler.scheduledTasks.size shouldEqual 1

    // fire the timed event to load from directory
    scheduler.scheduledTasks.head.run()
    eventSourceManager.eventSource.size shouldEqual 2

    var es = eventSourceManager.eventSource.filter(_.source.appId == "application_1462781278026_205691").head
    es.source.appId shouldEqual "application_1462781278026_205691"
    es.progress.eventProgress.hasNext shouldEqual false
    es.progress.eventProgress.hasPrevious shouldEqual true

    es = eventSourceManager.eventSource.filter(_.source.appId == "application_1472176676028_116806").head
    es.source.appId shouldEqual "application_1472176676028_116806"
    es.progress.eventProgress.hasNext shouldEqual false
    es.progress.eventProgress.hasPrevious shouldEqual true
  }

  it should "refresh with the latest new files when task fired" in {
    val args = Seq("-d", dirname).toArray
    config.parseCliArgs(args)
    server.buildEventSources()
    server.run()

    eventSourceManager.eventSource.size shouldEqual 0
    scheduler.scheduledTasks.size shouldEqual 1

    // fire the timed event to load from directory
    scheduler.scheduledTasks.head.run()
    eventSourceManager.eventSource.size shouldEqual 2

    eventSourceManager.eventSource.count(_.source.appId == "application_1462781278026_205691") shouldEqual 1
    eventSourceManager.eventSource.count(_.source.appId == "application_1472176676028_116806") shouldEqual 1

    // add a new file to the directory
    addInTempFile(tempFile)

    eventSourceManager.eventSource.size shouldEqual 2
    scheduler.scheduledTasks.head.run()
    eventSourceManager.eventSource.size shouldEqual 3

    eventSourceManager.eventSource.count(_.source.appId == "application_1462781278026_205691") shouldEqual 1
    eventSourceManager.eventSource.count(_.source.appId == "application_1472176676028_116806") shouldEqual 1
    eventSourceManager.eventSource.count(_.source.appId == "temp_addded_in_test") shouldEqual 1

    // cleanup again
    cleanupTempFile(tempFile)
  }

  it should "new files should auto play if configured" in {
    val args = Seq("-d", dirname, "-r").toArray
    config.parseCliArgs(args)
    server.buildEventSources()
    server.run()

    // fire the timed event to load from directory
    scheduler.scheduledTasks.head.run()
    eventSourceManager.eventSource.size shouldEqual 2

    // add a new file to the directory
    addInTempFile(tempFile)

    scheduler.scheduledTasks.head.run()
    eventSourceManager.eventSource.size shouldEqual 3

    val es = eventSourceManager.eventSource.filter(_.source.appId == "temp_addded_in_test").head
    es.source.appId shouldEqual "temp_addded_in_test"
    es.progress.eventProgress.hasNext shouldEqual false
    es.progress.eventProgress.hasPrevious shouldEqual true

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

case class StubEventSourceManager(eventSource: ArrayBuffer[EventSourceDetail] = ArrayBuffer[EventSourceDetail]())
  extends EventSourceManagerLike {

  override def addEventSource(eventDetail: EventSourceDetail): Unit = eventSource += eventDetail

  override def sourceCount: Int = eventSource.size

  override def containsAppId(appId: String): Boolean = eventSource.exists(_.source.appId == appId)

  @throws[NoSuchElementException]
  override def getSource(appId: String): EventSourceLike = ???

  @throws[NoSuchElementException]
  override def getScrollingSource(appId: String): FreeScrollEventSource = ???

  @throws[NoSuchElementException]
  override def getSourceDetail(appId: String): EventSourceDetail = ???
}

case class StubScheduler(scheduledTasks: ArrayBuffer[ScheduledTask[_]] = ArrayBuffer[ScheduledTask[_]]())
  extends SchedulerLike {

  override def scheduleTask[T](task: ScheduledTask[T]): Unit = scheduledTasks += task

  override def cancelAll(): Unit = ???
}
