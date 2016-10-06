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
import org.apache.spark.scheduler._

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
  extends EventSourceBase(eventState)
    with EventSourceLike with CanFreeScroll with Logging {

  // cache data to internal buffer and set initial state
  private val buffer = new EventBuffer(fillBuffer())
  primeState()

  override def appId: String = eventState.getState.appId.getOrElse(fileSource.getName)

  @throws[NoSuchElementException]
  override def forward(count: Int = 1): EventSourceProgress = {
    require(count > 0)
    (0 until count).foreach(i => forward())
    progress
  }

  @throws[NoSuchElementException]
  override def rewind(count: Int = 1): EventSourceProgress = {
    require(count >= 0)
    (0 until count).foreach(i => backward())
    progress
  }

  @throws[NoSuchElementException]
  override def end(): EventSourceProgress = {
    while (progress.hasNext) forward()
    progress
  }

  @throws[NoSuchElementException]
  override def start(): EventSourceProgress = {
    while (progress.hasPrevious) backward()
    progress
  }

  override def progress: EventSourceProgress = buffer.progress

  override def state: SparklintStateLike = eventState.getState

  private def forward() = eventState.onEvent(buffer.next)

  private def backward() = eventState.unEvent(buffer.previous)

  private def fillBuffer(): IndexedSeq[SparkListenerEvent] = {
    Try(Source.fromFile(fileSource)) match {
      case Success(sparkEventLog) =>
        sparkEventLog.getLines().flatMap(toStateOrBuffer).toIndexedSeq
      case Failure(ex)            =>
        throw new IllegalArgumentException(s"Failure reading file event source from ${fileSource.getName}.", ex)
    }
  }

  private def primeState() = {
    // prime the state by loading the first message
    if (buffer.hasNext) {
      forward()
      logInfo(s"Loaded and primed event source $fullName: ${buffer.progress} ")
    } else {
      logWarn(s"Empty buffer detected for event source id $appId")
    }
  }

  private def toStateOrBuffer(line: String): Option[SparkListenerEvent] = {
    StringToSparkEvent(line) match {
      case event: SparkListenerLogStartShim      => setVersionState(event)
      case event: SparkListenerBlockManagerAdded => setBlockManagerState(event)
      case event: SparkListenerEnvironmentUpdate => setEnvironmentState(event)
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

}

object FileEventSource {
  def apply(fileSource: File, runImmediately: Boolean): Option[FileEventSource] = Try {
    val eventSource = new FileEventSource(fileSource, new LosslessEventState())
    if (runImmediately) {
      logInfo(s"Auto playing event source ${eventSource.fullName}")
      while (!eventSource.progress.atEnd) {
        eventSource.forward()
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