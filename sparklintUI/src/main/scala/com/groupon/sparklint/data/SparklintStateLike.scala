package com.groupon.sparklint.data

/**
  * @author rxue
  * @since 9/23/16.
  */
trait SparklintStateLike {

  def executorInfo: Map[String, SparklintExecutorInfo]

  def stageMetrics: Map[SparklintStageIdentifier, SparklintStageMetrics]

  def runningTasks: Map[Long, SparklintTaskInfo]

  def firstTaskAt: Option[Long]

  def lastUpdatedAt: Long

  def stageIdLookup: Map[Int, SparklintStageIdentifier]

  def applicationEndedAt: Option[Long]

  /**
    * TaskLocality -> CPU usage
    * @return
    */
  def coreUsage: Map[Symbol, MetricsSink]

  lazy val aggregatedCoreUsage: MetricsSink = {
    MetricsSink.mergeSinks(coreUsage.values)
  }
}
