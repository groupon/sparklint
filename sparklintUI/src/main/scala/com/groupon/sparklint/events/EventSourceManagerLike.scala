package com.groupon.sparklint.events

/**
  * Implementations of this trait are capable of managing the list of event sources for a specific configuration of
  * Sparklint.
  *
  * @author swhitear
  * @since 9/13/16.
  */
trait EventSourceManagerLike {

  /**
    * The number of sources currently in the manager.
    *
    * @return The count of EventSourceLike instances in the manager.
    */
  def sourceCount: Int

  /**
    * An Iterable of EventSourceDetail instances returned in their insertion order.
    *
    * @return
    */
  def eventSourceDetails: Iterable[EventSourceDetail]

  /** True if the current set of managed EventSourceLike instances contains the specified appId.
    *
    * @param appId The appId to check for.
    * @return True if it exists, false otherwise.
    */
  def containsEventSourceId(appId: String): Boolean

  /**
    * Provides indexed access to the EventSourceDetail instances by appId.
    *
    * @param appId The appId of the EventSourceDetail instance to return.
    * @throws NoSuchElementException When the specified appId does not exist.
    * @return The specified EventSourceDetail instance wrapping hte EventSource and associated receivers.
    */
  @throws[NoSuchElementException]
  def getSourceDetail(appId: String): EventSourceDetail

  /**
    * Provides indexed access to any wrapped EventSourceLike instances that extend FreeScrollEventSource.
    *
    * @param appId The appId of the EventSourceDetail instance to return.
    * @throws NoSuchElementException When the specified appId does not exist.
    * @return
    */
  @throws[NoSuchElementException]
  def getScrollingSource(appId: String): FreeScrollEventSource
}
