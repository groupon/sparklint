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

  // important to declare this before the buffer is filled
  private var extractedId: Option[String] = None

  private val buffer  = new EventBuffer(fillBuffer())
  private val fScroll = new ScrollHandler(buffer.next, eventState.onEvent, !buffer.hasNext, progress)
  private val bScroll = new ScrollHandler(buffer.previous, eventState.unEvent, !buffer.hasPrevious, progress)

  override val appId: String = extractedId.getOrElse(fileSource.getName)

  @throws[IllegalArgumentException]
  def forwardEvents(count: Int = 1) = fScroll.scroll(count)

  @throws[IllegalArgumentException]
  def rewindEvents(count: Int = 1) = bScroll.scroll(count)

  @throws[IllegalArgumentException]
  def forwardTasks(count: Int = 1) = fScroll.scroll(count, (evt) => evt.isInstanceOf[SparkListenerTaskEnd])

  @throws[IllegalArgumentException]
  def rewindTasks(count: Int = 1) = bScroll.scroll(count, (evt) => evt.isInstanceOf[SparkListenerTaskStart])

  @throws[IllegalArgumentException]
  def forwardStages(count: Int = 1) = fScroll.scroll(count, (evt) => evt.isInstanceOf[SparkListenerStageCompleted])

  @throws[IllegalArgumentException]
  def rewindStages(count: Int = 1) = bScroll.scroll(count, (evt) => evt.isInstanceOf[SparkListenerStageSubmitted])

  @throws[IllegalArgumentException]
  def forwardJobs(count: Int = 1) = fScroll.scroll(count, (evt) => evt.isInstanceOf[SparkListenerJobEnd])

  @throws[IllegalArgumentException]
  def rewindJobs(count: Int = 1) = bScroll.scroll(count, (evt) => evt.isInstanceOf[SparkListenerJobStart])

  override def toEnd(): EventSourceProgress = fScroll.scroll(Int.MaxValue)

  override def toStart(): EventSourceProgress = bScroll.scroll(Int.MaxValue)

  override def progress: EventSourceProgress = EventSourceProgress(buffer.eventCount, buffer.index)

  override def state: SparklintStateLike = eventState.getState

  private def fillBuffer(): IndexedSeq[SparkListenerEvent] = {
    Try(Source.fromFile(fileSource)) match {
      case Success(sparkEventLog) =>
        sparkEventLog.getLines().flatMap(toStateOrBuffer).toIndexedSeq
      case Failure(ex)            =>
        throw new IllegalArgumentException(s"Failure reading file event source from ${fileSource.getName}.", ex)
    }
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
      while (eventSource.progress.hasNext) {
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

private class ScrollHandler(movefn: () => SparkListenerEvent,
                            statefn: (SparkListenerEvent) => Unit,
                            breaker: => Boolean,
                            progress: => EventSourceProgress) {

  @throws[scala.IllegalArgumentException]
  def scroll(count: Int, decrementfn: (SparkListenerEvent) => Boolean = (evt) => true): EventSourceProgress = {
    require(count >= 0)

    var counter = count

    def decrementIfMatch(event: SparkListenerEvent): SparkListenerEvent = {
      if (decrementfn(event)) counter -= 1
      event
    }

    while (counter > 0 && !breaker) {
      statefn(decrementIfMatch(movefn()))
    }

    progress
  }
}

