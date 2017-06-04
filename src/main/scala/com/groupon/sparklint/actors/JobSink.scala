/*
 * Copyright 2016 Groupon, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.groupon.sparklint.actors

import akka.actor.{Actor, ActorRef, Props}
import org.apache.spark.scheduler.TaskLocality.TaskLocality
import org.apache.spark.scheduler._

import scala.collection.{SortedMap, mutable}

/**
  * @author rxue
  * @since 6/3/17.
  */
object JobSink {
  val name: String = "job"

  def props: Props = Props(new JobSink)

  case class GetCoreUsageByLocality(numOfBuckets: Int, since: Option[Long], until: Option[Long], replyTo: ActorRef)

  case class CoreUsageByLocalityResponse(byLocality: Map[TaskLocality, SortedMap[Long, Int]], availableCores: SortedMap[Long, Int])

  case class GetCoreUsageByJobGroup(numOfBuckets: Int, since: Option[Long], until: Option[Long], replyTo: ActorRef)

  case class CoreUsageByJobGroupResponse(byJobGroup: Map[String, SortedMap[Long, Int]], availableCores: SortedMap[Long, Int])

  case object NoDataYet

  case class GetCoreUtilizationByLocality(since: Option[Long], until: Option[Long], replyTo: ActorRef)

  case class CoreUtilizationByLocalityResponse(byLocality: Map[TaskLocality, Double])

  case class GetCoreUtilizationByJobGroup(since: Option[Long], untilNo: Option[Long], replyTo: ActorRef)

  case class CoreUtilizationByJobGroupResponse(byJobGroup: Map[String, Double])

  case class JobSummary(jobGroup: Option[String], jobDescription: Option[String], started: Long, ended: Option[Long] = None)

}

class JobSink extends Actor {

  import JobSink._

  implicit val usageSummaryOrdering: Ordering[UsageSummary] = UsageSummary
  private val stageToJob: mutable.Map[Int, Int] = mutable.Map()
  private val runningJobs: mutable.Map[Int, JobSummary] = mutable.Map()
  private val finishedJobs: mutable.Map[Int, JobSummary] = mutable.Map()
  private val metricsByLocality: Map[TaskLocality, LosslessMetricsSink] = TaskLocality.values.toSeq.map(locality =>
    locality -> new LosslessMetricsSink).toMap
  private val metricsByJobGroup: mutable.Map[String, LosslessMetricsSink] = mutable.Map.empty
  private val availableCoresMetrics = new LosslessMetricsSink()
  private val liveExecutors: mutable.Map[String, UsageSummary] = mutable.Map.empty
  private val runningTask: mutable.Map[Long, UsageSummary] = mutable.Map.empty

  override def receive: Receive = {
    case e: SparkListenerExecutorAdded =>
      val newExecutorSummary = UsageSummary(Some(e.executorId.hashCode), e.time, weight = e.executorInfo.totalCores)
      liveExecutors(e.executorId) = newExecutorSummary
      availableCoresMetrics.push(newExecutorSummary)
    case e: SparkListenerExecutorRemoved =>
      for (executorSummary <- liveExecutors.remove(e.executorId)) {
        executorSummary.end = Some(e.time)
      }
    case e: SparkListenerApplicationEnd =>
      for ((executorId, executorSummary) <- liveExecutors) {
        liveExecutors.remove(executorId)
        executorSummary.end = Some(e.time)
      }
      for ((jobId, jobSummary) <- runningJobs) {
        runningJobs.remove(jobId)
        finishedJobs(jobId) = jobSummary.copy(ended = Some(e.time))
      }
      for ((taskId, taskExecution) <- runningTask) {
        runningTask.remove(taskId)
        taskExecution.end = Some(e.time)
      }

    case e: SparkListenerJobStart =>
      runningJobs(e.jobId) = JobSummary(Option(e.properties.getProperty("spark.jobGroup.id")),
        Option(e.properties.getProperty("spark.job.description")), e.time)
      for (stageId <- e.stageIds) {
        stageToJob(stageId) = e.jobId
      }
    case e: SparkListenerJobEnd =>
      for (jobSummary <- runningJobs.remove(e.jobId)) {
        finishedJobs(e.jobId) = jobSummary.copy(ended = Some(e.time))
      }
      stageToJob.retain((_, jobId) => jobId != e.jobId)
    case e: SparkListenerStageSubmitted =>
    // ignore for now
    case e: SparkListenerStageCompleted =>
    // ignore for now
    case e: SparkListenerTaskStart =>
      val newTaskExecution = UsageSummary(Some(e.taskInfo.taskId), e.taskInfo.launchTime)
      runningTask(e.taskInfo.taskId) = newTaskExecution
      metricsByLocality(e.taskInfo.taskLocality).push(newTaskExecution)
      val jobGroup = stageToJob.get(e.stageId).flatMap(runningJobs.get).flatMap(_.jobGroup).getOrElse("default")
      metricsByJobGroup.getOrElseUpdate(jobGroup, new LosslessMetricsSink()).push(newTaskExecution)

    case e: SparkListenerTaskEnd =>
      for (taskExecution <- runningTask.remove(e.taskInfo.taskId)) {
        taskExecution.end = Some(e.taskInfo.finishTime)
      }

    case GetCoreUsageByLocality(numOfBuckets, since, until, replyTo) =>
      if (metricsByLocality.exists(_._2.nonEmpty)) {
        val earliestTime = since.getOrElse(metricsByLocality.flatMap(_._2.earliestTime).min)
        val latestTime = until.getOrElse(metricsByLocality.flatMap(_._2.latestTime).max)
        if (earliestTime >= latestTime) {
          replyTo ! NoDataYet
        } else {
          val coreUsageByLocality = metricsByLocality.mapValues(_.collect(earliestTime, numOfBuckets, latestTime))
          val availableCores = availableCoresMetrics.collect(earliestTime, numOfBuckets, latestTime)
          replyTo ! CoreUsageByLocalityResponse(coreUsageByLocality, availableCores)
        }
      } else {
        replyTo ! NoDataYet
      }

    case GetCoreUsageByJobGroup(numOfBuckets, since, until, replyTo) =>
      if (metricsByJobGroup.exists(_._2.nonEmpty)) {
        val earliestTime = since.getOrElse(metricsByJobGroup.flatMap(_._2.earliestTime).min)
        val latestTime = until.getOrElse(metricsByJobGroup.flatMap(_._2.latestTime).max)
        if (earliestTime >= latestTime) {
          replyTo ! NoDataYet
        } else {
          val coreUsageByJobGroup = metricsByJobGroup.mapValues(_.collect(earliestTime, numOfBuckets, latestTime)).toMap
          val availableCores = availableCoresMetrics.collect(earliestTime, numOfBuckets, latestTime)
          replyTo ! CoreUsageByJobGroupResponse(coreUsageByJobGroup, availableCores)
        }
      } else {
        replyTo ! NoDataYet
      }

    case GetCoreUtilizationByLocality(since, until, replyTo) =>
      if (availableCoresMetrics.nonEmpty && metricsByLocality.exists(_._2.nonEmpty)) {
        val earliestTime = since.getOrElse(metricsByLocality.flatMap(_._2.earliestTime).min)
        val latestTime = until.getOrElse(metricsByLocality.flatMap(_._2.latestTime).max)
        val totalCoreSeconds = availableCoresMetrics.sum(earliestTime, latestTime)
        if (totalCoreSeconds == 0) {
        } else {
          replyTo ! CoreUtilizationByLocalityResponse(metricsByLocality.mapValues(_.sum(earliestTime, latestTime).toDouble / totalCoreSeconds))
        }
      } else {
        replyTo ! NoDataYet
      }

    case GetCoreUtilizationByJobGroup(since, until, replyTo) =>
      if (availableCoresMetrics.nonEmpty && metricsByJobGroup.exists(_._2.nonEmpty)) {
        val earliestTime = since.getOrElse(metricsByJobGroup.flatMap(_._2.earliestTime).min)
        val latestTime = until.getOrElse(metricsByJobGroup.flatMap(_._2.latestTime).max)
        val totalCoreSeconds = availableCoresMetrics.sum(earliestTime, latestTime)
        if (totalCoreSeconds == 0) {
        } else {
          replyTo ! CoreUtilizationByJobGroupResponse(metricsByJobGroup.mapValues(_.sum(earliestTime, latestTime).toDouble / totalCoreSeconds).toMap)
        }
      } else {
        replyTo ! NoDataYet
      }
  }
}
