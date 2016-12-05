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

import com.groupon.sparklint.common.Logging
import org.apache.spark.groupon.StringToSparkEvent
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
case class FileEventSource(fileSource: File, receivers: Seq[EventReceiverLike])
  extends FreeScrollEventSource with Logging {

  private val buffer    = new EventBuffer(fillBuffer())
  private val fwdScroll = new ScrollHandler(buffer.next, onEvent, !buffer.hasNext)
  private val rewScroll = new ScrollHandler(buffer.previous, unEvent, !buffer.hasPrevious)

  override val eventSourceId: String = fileSource.getName

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
    Try(Source.fromFile(fileSource)) match {
      case Success(sparkEventLog) =>
        sparkEventLog.getLines().flatMap(parseAndPreprocess).toIndexedSeq
      case Failure(ex)            =>
        throw new IllegalArgumentException(s"Failure reading file event source from ${fileSource.getName}.", ex)
    }
  }

  private def parseAndPreprocess(line: String): Option[SparkListenerEvent] = {
    val event = StringToSparkEvent(line)
    preprocessEvent(event)
    Some(event)
  }

  private def preprocessEvent(event: SparkListenerEvent) = receivers.foreach(_.preprocess(event))

  private def onEvent(event: SparkListenerEvent) = receivers.foreach(_.onEvent(event))

  private def unEvent(event: SparkListenerEvent) = receivers.foreach(_.unEvent(event))

}

private class ScrollHandler(movefn: () => SparkListenerEvent,
                            statefn: (SparkListenerEvent) => Unit,
                            breaker: => Boolean) {

  @throws[scala.IllegalArgumentException]
  def scroll(count: Int, decrementfn: (SparkListenerEvent) => Boolean = (evt) => true) = {
    require(count >= 0)

    var counter = count

    def decrementIfMatch(event: SparkListenerEvent): SparkListenerEvent = {
      if (decrementfn(event)) counter -= 1
      event
    }

    while (counter > 0 && !breaker) {
      statefn(decrementIfMatch(movefn()))
    }
  }
}
