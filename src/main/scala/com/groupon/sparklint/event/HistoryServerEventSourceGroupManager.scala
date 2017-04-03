package com.groupon.sparklint.event

/**
  * @author rxue
  * @since 1.0.5
  */
class HistoryServerEventSourceGroupManager(api: HistoryServerApi) extends GenericEventSourceGroupManager(api.name, true) {

}
