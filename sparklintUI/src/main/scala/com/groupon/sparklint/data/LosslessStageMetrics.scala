package com.groupon.sparklint.data

/**
  * @author rxue
  * @since 9/22/16.
  */
case class LosslessStageMetrics(losslessMetricsRepo: Map[(Symbol, Symbol), LosslessTaskCounter])
  extends SparklintStageMetrics {
  override def metricsRepo: Map[(Symbol, Symbol), SparklintTaskCounter] = losslessMetricsRepo

  def merge(taskId: Long, taskType: Symbol, locality: Symbol, metrics: SparklintTaskMetrics): LosslessStageMetrics = {
    val newMetrics = losslessMetricsRepo.getOrElse(locality -> taskType, new LosslessTaskCounter).merge(taskId, metrics)
    copy(losslessMetricsRepo + ((locality -> taskType) -> newMetrics))
  }
}

object LosslessStageMetrics {
  def empty: LosslessStageMetrics = new LosslessStageMetrics(Map.empty)
}
