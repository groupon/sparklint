package org.apache.spark.groupon

import org.apache.spark.scheduler.SparkListenerLogStart

/**
  * A shim to allow handling of private[spark] LogStart event in the rest of Sparklint.
  * @author swhitear
  * @since 9/30/16.
  */
class SparkListenerLogStartShim(sourceEvent: SparkListenerLogStart)
  extends SparkListenerLogStart(sourceEvent.sparkVersion)
