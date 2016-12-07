package com.groupon.sparklint.common

import org.apache.spark.SparkConf

/**
  * @author rxue
  * @since 12/7/16.
  */
class SparkConfSparklintConfig(conf: SparkConf) extends SparklintConfig {
  override def port: Int = conf.get("sparklint.port", defaultPort.toString).toInt
}
