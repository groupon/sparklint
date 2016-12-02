package com.groupon.sparklint.data

/**
  * @author rxue
  * @since 8/16/16.
  */
case class CompressedStageMetrics(compressedMetricsRepo: Map[(Symbol, Symbol), CompressedTaskCounter])
  extends SparklintStageMetrics {
  def merge(taskId: Long, taskType: Symbol, locality: Symbol, metrics: SparklintTaskMetrics): CompressedStageMetrics = {
    val newMetrics = compressedMetricsRepo.getOrElse(locality -> taskType, new CompressedTaskCounter).merge(taskId, metrics)
    copy(compressedMetricsRepo + ((locality -> taskType) -> newMetrics))
  }

  override def metricsRepo: Map[(Symbol, Symbol), SparklintTaskCounter] = compressedMetricsRepo
}

object CompressedStageMetrics {
  def empty: CompressedStageMetrics = new CompressedStageMetrics(Map.empty)
}
