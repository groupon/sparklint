package com.groupon.sparklint.event

import scala.collection.mutable.ListBuffer

/**
  * @author rxue
  * @since 1.0.5
  */
class GenericEventSourceGroupManager(override val name: String, override val closeable: Boolean) extends EventSourceGroupManager {
  private val esList: ListBuffer[EventSource] = ListBuffer.empty

  /**
    * Register an event source
    *
    * @param es the [[EventSource]] to register
    * @return true if success, false if the event source has been registered already
    */
  def registerEventSource(es: EventSource): Boolean = {
    if (esList.exists(_.appMeta == es.appMeta)) {
      false
    } else {
      esList.append(es)
      true
    }
  }

  override def eventSources: Seq[EventSource] = esList
}
