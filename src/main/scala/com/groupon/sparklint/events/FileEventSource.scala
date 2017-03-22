/*
 * Copyright 2016 Groupon, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.groupon.sparklint.events

import java.io.File

import org.apache.spark.groupon.StringToSparkEvent
import org.apache.spark.scheduler._

import scala.io.Source
import scala.util.{Failure, Success, Try}

/**
  * @author rxue,swhitear
  * @since 8/18/16.
  */
case class FileEventSource(file: File) extends EventSourceLike with FreeScrollEventSource {

  private val _progress = new EventProgressTracker()
  private val _state    = new LosslessStateManager()
  private val _meta     = EventSourceMeta.fromFile(file)

  private class ScrollHandler(scrollNextEvent: => SparkListenerEvent,
                              updateState: (SparkListenerEvent) => Unit,
                              breaker: => Boolean) {
    @throws[scala.IllegalArgumentException]
    def scroll(milestones: Int, isMilestone: (SparkListenerEvent) => Boolean = (_) => true): Unit = {
      require(milestones >= 0)

      var remainingMilestones = milestones

      while (remainingMilestones > 0 && !breaker) {
        scrollNextEvent match {
          case e if isMilestone(e) => remainingMilestones -= 1
            updateState(e)
          case e                   => updateState(e)
        }
      }
    }
  }

  override def identifier: EventSourceIdentifier = meta.appIdentifier

  override def friendlyName: String = meta.appName

  override protected def receivers: Seq[EventReceiverLike] = Seq(_meta, _progress, _state)

  override def progress: EventProgressTrackerLike = _progress

  override def meta: EventSourceMetaLike = _meta

  override def state: EventStateManagerLike = _state

  override def getEventSourceDetail: EventSourceDetail = EventSourceDetail(_meta, _progress, _state)

  private val buffer    = new EventBuffer(fillBuffer())
  private val fwdScroll = new ScrollHandler(buffer.next, onEvent, !buffer.hasNext)
  private val rewScroll = new ScrollHandler(buffer.previous, unEvent, !buffer.hasPrevious)

  @throws[IllegalArgumentException]
  def forwardEvents(count: Int = 1): Unit = fwdScroll.scroll(count)

  @throws[IllegalArgumentException]
  def rewindEvents(count: Int = 1): Unit = rewScroll.scroll(count)

  @throws[IllegalArgumentException]
  def forwardTasks(count: Int = 1): Unit = fwdScroll.scroll(count, (evt) => evt.isInstanceOf[SparkListenerTaskEnd])

  @throws[IllegalArgumentException]
  def rewindTasks(count: Int = 1): Unit = rewScroll.scroll(count, (evt) => evt.isInstanceOf[SparkListenerTaskStart])

  @throws[IllegalArgumentException]
  def forwardStages(count: Int = 1): Unit = fwdScroll.scroll(count, (evt) => evt.isInstanceOf[SparkListenerStageCompleted])

  @throws[IllegalArgumentException]
  def rewindStages(count: Int = 1): Unit = rewScroll.scroll(count, (evt) => evt.isInstanceOf[SparkListenerStageSubmitted])

  @throws[IllegalArgumentException]
  def forwardJobs(count: Int = 1): Unit = fwdScroll.scroll(count, (evt) => evt.isInstanceOf[SparkListenerJobEnd])

  @throws[IllegalArgumentException]
  def rewindJobs(count: Int = 1): Unit = rewScroll.scroll(count, (evt) => evt.isInstanceOf[SparkListenerJobStart])

  override def toEnd(): Unit = fwdScroll.scroll(Int.MaxValue)

  override def toStart(): Unit = rewScroll.scroll(Int.MaxValue)

  override def hasNext: Boolean = buffer.hasNext

  override def hasPrevious: Boolean = buffer.hasPrevious

  private def fillBuffer(): IndexedSeq[SparkListenerEvent] = {
    Try(Source.fromFile(file)) match {
      case Success(sparkEventLog) =>
        sparkEventLog.getLines().flatMap(parseAndPreprocess).toIndexedSeq
      case Failure(ex)            =>
        throw new IllegalArgumentException(s"Failure reading file event source from ${file.getName}.", ex)
    }
  }

  private def parseAndPreprocess(line: String): Option[SparkListenerEvent] = {
    val event = StringToSparkEvent(line)
    event.foreach(preprocessEvent)
    event
  }

  private def preprocessEvent(event: SparkListenerEvent) = receivers.foreach(_.preprocess(event))

  private def onEvent(event: SparkListenerEvent) = receivers.foreach(_.onEvent(event))

  private def unEvent(event: SparkListenerEvent) = receivers.foreach(_.unEvent(event))

}
