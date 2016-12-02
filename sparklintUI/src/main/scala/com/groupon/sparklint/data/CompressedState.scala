package com.groupon.sparklint.data

/**
  * @author rxue
  * @since 8/22/16.
  */
case class CompressedState(coreUsage: Map[Symbol, CompressedMetricsSink],
                           executorInfo: Map[String, SparklintExecutorInfo],
                           stageMetrics: Map[SparklintStageIdentifier, CompressedStageMetrics],
                           stageIdLookup: Map[Int, SparklintStageIdentifier],
                           runningTasks: Map[Long, SparklintTaskInfo],
                           firstTaskAt: Option[Long],
                           applicationEndedAt: Option[Long],
                           lastUpdatedAt: Long) extends SparklintStateLike

object CompressedState {
  def empty: CompressedState = new CompressedState(Map.empty, Map.empty, Map.empty, Map.empty, Map.empty, None, None, 0L)
}
