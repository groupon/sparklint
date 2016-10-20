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

import java.io.File

import com.groupon.sparklint.SparklintServer._
import com.groupon.sparklint.common.Logging
import com.groupon.sparklint.data.SparklintStateLike
import org.apache.spark.groupon.{SparkListenerLogStartShim, StringToSparkEvent}
import org.apache.spark.scheduler.{SparkListenerEvent, SparkListenerTaskEnd, _}

import scala.io.Source
import scala.util.{Failure, Success, Try}

/**
  * The FileEventSource uses a file to populate an internal buffer that can then be used as an EventSourceLike
  * implementation.
  *
  * @author swhitear
  * @since 8/18/16.
  */
@throws[IllegalArgumentException]
case class FileEventSource(fileSource: File, eventState: EventStateLike)
  extends EventSourceBase(eventState) with FreeScrollEventSource with Logging {

  // cache data to internal buffer and set initial state
  private var extractedId: Option[String] = None  // important to declare this before the buffer is filled
  private val buffer = new EventBuffer(fillBuffer())

  override val appId: String = extractedId.getOrElse(fileSource.getName)

  @throws[IllegalArgumentException]
  def forwardEvents(count: Int = 1): EventSourceProgress = {
    require(count > 0)
    require(!progress.atEnd)
    scroll(new EventScrollHandler(count, buffer.next, eventState.onEvent), progress.atEnd)
  }

  @throws[IllegalArgumentException]
  def rewindEvents(count: Int = 1): EventSourceProgress = {
    require(count > 0)
    require(!progress.atStart)
    scroll(new EventScrollHandler(count, buffer.previous, eventState.unEvent), progress.atStart)
  }

  @throws[IllegalArgumentException]
  def forwardTasks(count: Int = 1): EventSourceProgress = {
    require(count > 0)
    require(!progress.atEnd)
    scroll(new TaskScrollHandler(count, buffer.next, eventState.onEvent), progress.atEnd)
  }

  @throws[IllegalArgumentException]
  def rewindTasks(count: Int = 1): EventSourceProgress = {
    require(count > 0)
    require(!progress.atStart)
    logInfo(s"REWINDING $count")
    scroll(new TaskScrollHandler(count, buffer.previous, eventState.unEvent), progress.atStart)
  }

  override def toEnd(): EventSourceProgress = {
    scroll(new EventScrollHandler(Int.MaxValue, buffer.next, eventState.onEvent), progress.atEnd)
  }

  override def toStart(): EventSourceProgress = {
    scroll(new EventScrollHandler(Int.MaxValue, buffer.previous, eventState.unEvent), progress.atStart)
  }

  override def progress: EventSourceProgress = buffer.progress

  override def state: SparklintStateLike = eventState.getState

  private def fillBuffer(): IndexedSeq[SparkListenerEvent] = {
    Try(Source.fromFile(fileSource)) match {
      case Success(sparkEventLog) =>
        sparkEventLog.getLines().flatMap(toStateOrBuffer).toIndexedSeq
      case Failure(ex)            =>
        throw new IllegalArgumentException(s"Failure reading file event source from ${fileSource.getName}.", ex)
    }
  }

  private def scroll(scroller: ScrollHandler, breaker: => Boolean): EventSourceProgress = {
    while (!scroller.atTarget && !breaker) {
      scroller.scroll()
    }
    progress
  }

  private def toStateOrBuffer(line: String): Option[SparkListenerEvent] = {
    StringToSparkEvent(line) match {
      case event: SparkListenerLogStartShim      => setVersionState(event)
      case event: SparkListenerBlockManagerAdded => setBlockManagerState(event)
      case event: SparkListenerEnvironmentUpdate => setEnvironmentState(event)
      case event: SparkListenerApplicationStart  => setAppStartState(event)
      case event: SparkListenerApplicationEnd    => setAppEndState(event)
      case default                               => Some(default)
    }
  }

  private def setVersionState(event: SparkListenerLogStartShim): Option[SparkListenerEvent] = {
    versionOpt = Some(event.sparkVersion)
    None // filter the event from the buffer
  }

  private def setBlockManagerState(event: SparkListenerBlockManagerAdded): Option[SparkListenerEvent] = {
    hostOpt = Some(event.blockManagerId.host)
    portOpt = Some(event.blockManagerId.port)
    maxMemoryOpt = Some(event.maxMem)
    None // filter the event from the buffer
  }

  private def setEnvironmentState(event: SparkListenerEnvironmentUpdate): Option[SparkListenerEvent] = {
    environmentOpt = Some(event.environmentDetails)
    None // filter the event from the buffer
  }

  private def setAppStartState(event: SparkListenerApplicationStart): Option[SparkListenerEvent] = {
    extractedId = event.appId
    appNameOpt = Some(event.appName)
    userOpt = Some(event.sparkUser)
    startTimeOpt = Some(event.time)
    event // include the event in the buffer
  }

  private def setAppEndState(event: SparkListenerApplicationEnd): Option[SparkListenerEvent] = {
    endTimeOpt = Some(event.time)
    event // include the event in the buffer
  }

}

object FileEventSource {
  def apply(fileSource: File, runImmediately: Boolean): Option[FileEventSource] = Try {
    val eventSource = new FileEventSource(fileSource, new LosslessEventState())
    if (runImmediately) {
      logInfo(s"Auto playing event source ${eventSource.fullName}")
      while (!eventSource.progress.atEnd) {
        eventSource.forwardEvents()
      }
    }
    eventSource
  } match {
    case Success(eventSource) =>
      logInfo(s"Successfully created file source ${fileSource.getName}")
      Some(eventSource)
    case Failure(ex)          =>
      logWarn(s"Failure creating file source from ${fileSource.getName}: ${ex.getMessage}")
      None
  }
}

private class ScrollHandler(count: Int, movefn: () => SparkListenerEvent, statefn: (SparkListenerEvent) => Unit) {
  protected var counter = count

  protected def decrementIfMatch(event: SparkListenerEvent): SparkListenerEvent = event

  @throws[scala.IllegalArgumentException]
  def scroll(): Unit = {
    if (atTarget) throw new IllegalArgumentException("Cannot scroll, target reached.")
    statefn(decrementIfMatch(movefn()))
  }

  def atTarget: Boolean = counter == 0
}

private class EventScrollHandler(count: Int, movefn: () => SparkListenerEvent, statefn: (SparkListenerEvent) => Unit)
  extends ScrollHandler(count, movefn, statefn) {
  override def decrementIfMatch(event: SparkListenerEvent): SparkListenerEvent = {
    counter -= 1
    event
  }
}

private class TaskScrollHandler(count: Int, movefn: () => SparkListenerEvent, statefn: (SparkListenerEvent) => Unit)
  extends ScrollHandler(count, movefn, statefn) {
  override def decrementIfMatch(event: SparkListenerEvent): SparkListenerEvent = {
    if (event.isInstanceOf[SparkListenerTaskEnd]) counter -= 1
    event
  }
}

