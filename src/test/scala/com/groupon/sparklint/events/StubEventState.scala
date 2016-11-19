package com.groupon.sparklint.events

import com.groupon.sparklint.data.SparklintStateLike
import com.groupon.sparklint.data.compressed.CompressedState
import org.apache.spark.scheduler._

import scala.collection.mutable.ArrayBuffer

/**
  *
  * @author swhitear 
  * @since 11/16/16.
  */
class StubEventState(val onEvents: ArrayBuffer[SparkListenerEvent] = ArrayBuffer.empty,
                     val unEvents: ArrayBuffer[SparkListenerEvent] = ArrayBuffer.empty,
                     val state: SparklintStateLike = CompressedState.empty)
  extends EventStateLike {

  val preprocEvents = ArrayBuffer.empty[SparkListenerEvent]

  def eventCount = preprocEvents.size + onEvents.size + unEvents.size

  def preprocCount = preprocEvents.size

  def onCount = onEvents.size

  def unCount = unEvents.size

  override def onEvent(event: SparkListenerEvent): Unit = onEvents += event

  override def unEvent(event: SparkListenerEvent): Unit = unEvents += event

  override def getState: SparklintStateLike = state

  override def preprocAddApp(event: SparkListenerApplicationStart): Unit = appendPreproc(event)

  override def preprocAddExecutor(event: SparkListenerExecutorAdded): Unit = appendPreproc(event)

  override def preprocRemoveExecutor(event: SparkListenerExecutorRemoved): Unit = appendPreproc(event)

  override def preprocAddBlockManager(event: SparkListenerBlockManagerAdded): Unit = appendPreproc(event)

  override def preprocJobStart(event: SparkListenerJobStart): Unit = appendPreproc(event)

  override def preprocStageSubmitted(event: SparkListenerStageSubmitted): Unit = appendPreproc(event)

  override def preprocTaskStart(event: SparkListenerTaskStart): Unit = appendPreproc(event)

  override def preprocTaskEnd(event: SparkListenerTaskEnd): Unit = appendPreproc(event)

  override def preprocStageCompleted(event: SparkListenerStageCompleted): Unit = appendPreproc(event)

  override def preprocJobEnd(event: SparkListenerJobEnd): Unit = appendPreproc(event)

  override def preprocUnpersistRDD(event: SparkListenerUnpersistRDD): Unit = appendPreproc(event)

  override def preprocEndApp(event: SparkListenerApplicationEnd): Unit = appendPreproc(event)

  override def addApp(event: SparkListenerApplicationStart): Unit = appendOn(event)

  override def addExecutor(event: SparkListenerExecutorAdded): Unit = appendOn(event)

  override def removeExecutor(event: SparkListenerExecutorRemoved): Unit = appendOn(event)

  override def addBlockManager(event: SparkListenerBlockManagerAdded): Unit = appendOn(event)

  override def jobStart(event: SparkListenerJobStart): Unit = appendOn(event)

  override def stageSubmitted(event: SparkListenerStageSubmitted): Unit = appendOn(event)

  override def taskStart(event: SparkListenerTaskStart): Unit = appendOn(event)

  override def taskEnd(event: SparkListenerTaskEnd): Unit = appendOn(event)

  override def stageCompleted(event: SparkListenerStageCompleted): Unit = appendOn(event)

  override def jobEnd(event: SparkListenerJobEnd): Unit = appendOn(event)

  override def unpersistRDD(event: SparkListenerUnpersistRDD): Unit = appendOn(event)

  override def endApp(event: SparkListenerApplicationEnd): Unit = appendUn(event)

  override def unAddApp(event: SparkListenerApplicationStart): Unit = appendUn(event)

  override def unAddExecutor(event: SparkListenerExecutorAdded): Unit = appendUn(event)

  override def unRemoveExecutor(event: SparkListenerExecutorRemoved): Unit = appendUn(event)

  override def unAddBlockManager(event: SparkListenerBlockManagerAdded): Unit = appendUn(event)

  override def unJobStart(event: SparkListenerJobStart): Unit = appendUn(event)

  override def unStageSubmitted(event: SparkListenerStageSubmitted): Unit = appendUn(event)

  override def unTaskStart(event: SparkListenerTaskStart): Unit = appendUn(event)

  override def unTaskEnd(event: SparkListenerTaskEnd): Unit = appendUn(event)

  override def unStageCompleted(event: SparkListenerStageCompleted): Unit = appendUn(event)

  override def unJobEnd(event: SparkListenerJobEnd): Unit = appendUn(event)

  override def unUnpersistRDD(event: SparkListenerUnpersistRDD): Unit = appendUn(event)

  override def unEndApp(event: SparkListenerApplicationEnd): Unit = appendUn(event)

  private def appendPreproc(event: SparkListenerEvent) = preprocEvents += event

  private def appendOn(event: SparkListenerEvent) = onEvents += event

  private def appendUn(event: SparkListenerEvent) = unEvents += event

}
