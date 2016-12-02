package com.groupon.sparklint.events

/**
  * A case class wrapping the three current event receivers, allows views to access by name.
  *
  * @param eventSourceId the id of the event source associated with the detailed state info.
  * @param meta          an EventSourceMetaLike instance containing metadata about the application.
  * @param progress      an EventProgressTrackerLike instance containing progress of various event types.
  * @param state         an EventStateManagerLike containing the current aggregated event state.
  * @author swhitear
  * @since 9/13/16.
  *
  */
case class EventSourceDetail(eventSourceId: String,
                             meta: EventSourceMetaLike,
                             progress: EventProgressTrackerLike,
                             state: EventStateManagerLike)
