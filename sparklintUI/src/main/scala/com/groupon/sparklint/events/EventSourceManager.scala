package com.groupon.sparklint.events

import com.groupon.sparklint.common.Logging

import scala.collection.mutable

/**
  * The production implementation of EventSourceManagerLike. Manages and abstracts EventSourceLike instances
  * server side.
  *
  * @author swhitear
  * @since 8/18/16.
  */
class EventSourceManager(sourceDetails: SourceAndDetail*) extends EventSourceManagerLike with Logging {

  // this sync'ed LinkedHashMap is necessary because we want to ensure ordering of items in the manager, not the UI.
  // insertion order works well enough here, we have no need for any other guarantees from the data structure.
  private val eventSourcesByAppId = new mutable.LinkedHashMap[String, SourceAndDetail]()
    with mutable.SynchronizedMap[String, SourceAndDetail]

  sourceDetails.foreach(addEventSourceAndDetail)

  private[events] def addEventSourceAndDetail(sourceAndDetail: SourceAndDetail): EventSourceLike = {
    eventSourcesByAppId.put(sourceAndDetail.id, sourceAndDetail)
    sourceAndDetail.source
  }

  override def sourceCount: Int = eventSourcesByAppId.size

  override def eventSourceDetails: Iterable[EventSourceDetail] = eventSourcesByAppId.values.map(_.detail)

  override def containsEventSourceId(eventSourceId: String): Boolean = eventSourcesByAppId.contains(eventSourceId)

  @throws[NoSuchElementException]
  override def getSourceDetail(appId: String): EventSourceDetail = eventSourcesByAppId(appId).detail

  @throws[NoSuchElementException]
  override def getScrollingSource(appId: String): FreeScrollEventSource = {
    eventSourcesByAppId.get(appId) match {
      case Some(SourceAndDetail(source: FreeScrollEventSource, _)) => source
      case Some(_)                                                 => scrollFail(appId)
      case None                                                    => idFail(appId)
    }
  }

  private def scrollFail(appId: String) = throw new IllegalArgumentException(s"$appId cannot free scroll")

  private def idFail(appId: String) = throw new NoSuchElementException(s"Missing appId $appId")
}
