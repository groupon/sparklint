package com.groupon.sparklint.data

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.util.StatCounter

import scala.collection.mutable

/**
  * @author rxue
  * @since 12/4/16.
  */
object SparkVersionSpecificToSparklint {
  def sparklintTaskMetrics(m: TaskMetrics): SparklintTaskMetrics = {
    SparklintTaskMetrics(
      SparkToSparklint.sparklintOutputMetrics(m.outputMetrics),
      SparkToSparklint.sparklintInputMetrics(m.inputMetrics),
      SparkToSparklint.sparklintShuffleReadMetrics(m.shuffleReadMetrics),
      SparkToSparklint.sparklintShuffleWriteMetrics(m.shuffleWriteMetrics),
      m.diskBytesSpilled,
      m.memoryBytesSpilled,
      m.executorDeserializeTime,
      m.jvmGCTime,
      m.resultSerializationTime,
      m.resultSize,
      m.executorRunTime
    )
  }
}
