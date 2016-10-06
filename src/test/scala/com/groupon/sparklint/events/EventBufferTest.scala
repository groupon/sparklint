package com.groupon.sparklint.events

import java.util.function.Consumer

import com.groupon.sparklint.TestUtils._
import org.apache.spark.scheduler.{SparkListenerStageSubmitted, SparkListenerEvent}
import org.scalatest.{FlatSpec, Matchers}

/**
  * @author swhitear 
  * @since 9/13/16.
  */
class EventBufferTest extends FlatSpec with Matchers {

  it should "initialize fine with empty buffer" in {
    val buffer = new EventBuffer(IndexedSeq.empty)

    buffer.nextIndex shouldEqual 0
    buffer.previousIndex shouldEqual -1
    buffer.hasNext shouldBe false
    buffer.hasPrevious shouldBe false
    buffer.progress.count shouldEqual 0
    buffer.progress.position shouldEqual 0
    buffer.progress.atEnd shouldBe true
    buffer.progress.atStart shouldBe true
    buffer.progress.hasNext shouldBe false
    buffer.progress.hasPrevious shouldBe false
    buffer.progress.percent shouldEqual 0

    intercept[NoSuchElementException] {
      buffer.next()
    }

    intercept[NoSuchElementException] {
      buffer.previous()
    }
  }

  it should "not have set and add functions implemented yet" in {
    val buffer = new EventBuffer(IndexedSeq.empty)

    intercept[NotImplementedError] {
      buffer.set(sparkStageSubmittedEvent(42, "test_stage"))
    }

    intercept[NotImplementedError] {
      buffer.add(sparkStageSubmittedEvent(4242, "test_stage"))
    }
  }

  it should "allow for iteration forward and backward through the buffer" in {
    val buffer = new EventBuffer(genEvents(100))

    while (buffer.hasNext) {
      val index = buffer.nextIndex
      buffer.next.asInstanceOf[SparkListenerStageSubmitted].stageInfo.stageId shouldEqual index
    }

    while (buffer.hasPrevious) {
      val index = buffer.previousIndex
      buffer.previous.asInstanceOf[SparkListenerStageSubmitted].stageInfo.stageId shouldEqual index
    }
  }

  it should "report progress correctly" in {
    val buffer = new EventBuffer(genEvents(4))

    buffer.nextIndex shouldEqual 0
    buffer.previousIndex shouldEqual -1
    buffer.hasNext shouldBe true
    buffer.hasPrevious shouldBe false
    buffer.progress.count shouldEqual 4
    buffer.progress.position shouldEqual 0
    buffer.progress.atEnd shouldBe false
    buffer.progress.atStart shouldBe true
    buffer.progress.hasNext shouldBe true
    buffer.progress.hasPrevious shouldBe false
    buffer.progress.percent shouldEqual 0

    buffer.next
    buffer.nextIndex shouldEqual 1
    buffer.previousIndex shouldEqual 0
    buffer.hasNext shouldBe true
    buffer.hasPrevious shouldBe true
    buffer.progress.count shouldEqual 4
    buffer.progress.position shouldEqual 1
    buffer.progress.atEnd shouldBe false
    buffer.progress.atStart shouldBe false
    buffer.progress.hasNext shouldBe true
    buffer.progress.hasPrevious shouldBe true
    buffer.progress.percent shouldEqual 25

    buffer.next
    buffer.nextIndex shouldEqual 2
    buffer.previousIndex shouldEqual 1
    buffer.hasNext shouldBe true
    buffer.hasPrevious shouldBe true
    buffer.progress.count shouldEqual 4
    buffer.progress.position shouldEqual 2
    buffer.progress.atEnd shouldBe false
    buffer.progress.atStart shouldBe false
    buffer.progress.hasNext shouldBe true
    buffer.progress.hasPrevious shouldBe true
    buffer.progress.percent shouldEqual 50

    buffer.next
    buffer.nextIndex shouldEqual 3
    buffer.previousIndex shouldEqual 2
    buffer.hasNext shouldBe true
    buffer.hasPrevious shouldBe true
    buffer.progress.count shouldEqual 4
    buffer.progress.position shouldEqual 3
    buffer.progress.atEnd shouldBe false
    buffer.progress.atStart shouldBe false
    buffer.progress.hasNext shouldBe true
    buffer.progress.hasPrevious shouldBe true
    buffer.progress.percent shouldEqual 75

    buffer.next
    buffer.nextIndex shouldEqual 4
    buffer.previousIndex shouldEqual 3
    buffer.hasNext shouldBe false
    buffer.hasPrevious shouldBe true
    buffer.progress.count shouldEqual 4
    buffer.progress.position shouldEqual 4
    buffer.progress.atEnd shouldBe true
    buffer.progress.atStart shouldBe false
    buffer.progress.hasNext shouldBe false
    buffer.progress.hasPrevious shouldBe true
    buffer.progress.percent shouldEqual 100

    buffer.previous
    buffer.previous
    buffer.nextIndex shouldEqual 2
    buffer.previousIndex shouldEqual 1
    buffer.hasNext shouldBe true
    buffer.hasPrevious shouldBe true
    buffer.progress.count shouldEqual 4
    buffer.progress.position shouldEqual 2
    buffer.progress.atEnd shouldBe false
    buffer.progress.atStart shouldBe false
    buffer.progress.hasNext shouldBe true
    buffer.progress.hasPrevious shouldBe true
    buffer.progress.percent shouldEqual 50

    buffer.previous
    buffer.previous
    buffer.nextIndex shouldEqual 0
    buffer.previousIndex shouldEqual -1
    buffer.hasNext shouldBe true
    buffer.hasPrevious shouldBe false
    buffer.progress.count shouldEqual 4
    buffer.progress.position shouldEqual 0
    buffer.progress.atEnd shouldBe false
    buffer.progress.atStart shouldBe true
    buffer.progress.hasNext shouldBe true
    buffer.progress.hasPrevious shouldBe false
    buffer.progress.percent shouldEqual 0
  }


  private def genEvents(count: Int) = {
    (0 until count).map(i => sparkStageSubmittedEvent(i, "test_stage"))
  }

  class ConsoleBufferConsumer extends Consumer[SparkListenerEvent] {
    override def accept(ev: SparkListenerEvent): Unit = println(ev.toString)
  }
}
