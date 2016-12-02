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
      m.outputMetrics.map(SparkToSparklint.sparklintOutputMetrics).getOrElse(SparklintOutputMetrics()),
      m.inputMetrics.map(i => SparkToSparklint.sparklintInputMetrics(i)).getOrElse(SparklintInputMetrics()),
      m.shuffleReadMetrics.map(SparkToSparklint.sparklintShuffleReadMetrics).getOrElse(SparklintShuffleReadMetrics()),
      m.shuffleWriteMetrics.map(SparkToSparklint.sparklintShuffleWriteMetrics).getOrElse(SparklintShuffleWriteMetrics()),
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
