package com.groupon.sparklint.data

/**
  * @author rxue
  * @since 12/4/16.
  */
trait SparklintStageMetrics {
  /**
    * (TaskLocality, TaskType) -> TaskMetrics, to provide metrics group by TaskLocality and TaskType
    * @return
    */
  def metricsRepo: Map[(Symbol, Symbol), SparklintTaskCounter]

  def merge(taskId: Long, taskType: Symbol, locality: Symbol, metrics: SparklintTaskMetrics): SparklintStageMetrics
}
