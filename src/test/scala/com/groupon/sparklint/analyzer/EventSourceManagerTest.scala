package com.groupon.sparklint.analyzer

import com.groupon.sparklint.events._
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

/**
  * @author swhitear 
  * @since 11/21/16.
  */
class EventSourceManagerTest extends FlatSpec with Matchers with BeforeAndAfterEach {

  private var manager: EventSourceManager[String] = _

  override protected def beforeEach(): Unit = {
    manager = new EventSourceManager[String] {
      override def constructDetails(eventSourceCtor: String): Option[SourceAndDetail] = {
        val es = StubEventSource(eventSourceCtor, Seq.empty)
        val detail = EventSourceDetail(eventSourceCtor, new EventSourceMeta(),
          new EventProgressTracker(), new StubEventStateManager())
        Some(SourceAndDetail(es, detail))
      }
    }
  }

  it should "initialize empty" in {
    manager.sourceCount shouldEqual 0
    manager.eventSourceDetails.isEmpty shouldBe true
  }

  it should "add the event sources as expected and remain in order" in {
    manager.addEventSource("test_app_id_2")
    manager.addEventSource("test_app_id_1")

    manager.sourceCount shouldEqual 2
    manager.containsEventSourceId("test_app_id_1") shouldBe true
    manager.containsEventSourceId("test_app_id_2") shouldBe true
    manager.getSourceDetail("test_app_id_1").eventSourceId shouldBe "test_app_id_1"
    manager.getSourceDetail("test_app_id_2").eventSourceId shouldBe "test_app_id_2"
  }

  it should "throw up when invalid appId specified for indexer" in {
    manager.addEventSource("test_app_id")

    intercept[NoSuchElementException] {
      manager.getSourceDetail("invalid_app_id")
    }
  }
}
