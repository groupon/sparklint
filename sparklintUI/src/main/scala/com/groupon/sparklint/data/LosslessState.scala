package com.groupon.sparklint.data

/**
  * @author rxue
  * @since 9/22/16.
  */
case class LosslessState(coreUsage: Map[Symbol, LosslessMetricsSink],
                         executorInfo: Map[String, SparklintExecutorInfo],
                         stageMetrics: Map[SparklintStageIdentifier, LosslessStageMetrics],
                         stageIdLookup: Map[Int, SparklintStageIdentifier],
                         runningTasks: Map[Long, SparklintTaskInfo],
                         firstTaskAt: Option[Long],
                         applicationEndedAt: Option[Long],
                         lastUpdatedAt: Long) extends SparklintStateLike

object LosslessState {
  def empty: LosslessState = {
    new LosslessState(Map.empty, Map.empty, Map.empty, Map.empty, Map.empty, None, None, 0L)
  }
}
