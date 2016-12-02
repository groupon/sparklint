package com.groupon.sparklint.events

import com.groupon.sparklint.data.SparklintStateLike

/**
  * Extenders of the EventStateLike interface are capable of building current state based on the receiving
  * or removal of specific SparkListenerEvent messages.
  *
  * @author swhitear
  * @since 9/10/16.
  */
trait EventStateManagerLike {

  /**
    * The current EventState represented as a SparklintState object.
    *
    * @return the SparklintState object representing current state.
    */
  def getState: SparklintStateLike
}
