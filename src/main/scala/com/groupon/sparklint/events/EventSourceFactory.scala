package com.groupon.sparklint.events

/**
  * A simple trait to wrap a factory for constructing various combinations of EventSourceLike and their receivers.
  *
  * @author swhitear 
  * @since 11/18/16.
  */
trait EventSourceFactory[CtorT] {

  /**
    * Build a combination of EventSourceLike, EventStateLike and EventProgressLike objects.
    *
    * @param ctorVal the generic .ctor param for the concrete instance to construct the EventSourceDetail from.
    * @return the EventSourceDetail instance.
    */
  def buildEventSourceDetail(ctorVal: CtorT): Option[EventSourceDetail]

}
