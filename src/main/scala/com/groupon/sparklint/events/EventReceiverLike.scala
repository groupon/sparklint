/*
 Copyright 2016 Groupon, Inc.
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
 http://www.apache.org/licenses/LICENSE-2.0
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/
package com.groupon.sparklint.events

import org.apache.spark.scheduler._

/**
  * The EventReceiverLike interface provides base event routing and handling for all the event recievers
  * that are injected into an EventSource.
  *
  * All base methods are no-ops, implementers can chose to implement only those they need, and compose freely
  * from other traits.  Note, teh onPreprocEvent, onOnEvent and onUnEvent handlers are called for each inbound
  * preproc, on and un event before routing to the typed event handler for each, resulting in at least two
  * handler calls for each event type.
  *
  * @author swhitear 
  * @since 11/15/16.
  */
trait EventReceiverLike {

  // Always called event handlers of prepreoc, on and un

  def onPreprocEvent(event: SparkListenerEvent): Unit = {}

  def onOnEvent(event: SparkListenerEvent): Unit = {}

  def onUnEvent(event: SparkListenerEvent): Unit = {}


  // Preprocessing base no-ops

  def preprocAddApp(event: SparkListenerApplicationStart): Unit = {}

  def preprocAddExecutor(event: SparkListenerExecutorAdded): Unit = {}

  def preprocRemoveExecutor(event: SparkListenerExecutorRemoved): Unit = {}

  def preprocAddBlockManager(event: SparkListenerBlockManagerAdded): Unit = {}

  def preprocJobStart(event: SparkListenerJobStart): Unit = {}

  def preprocStageSubmitted(event: SparkListenerStageSubmitted): Unit = {}

  def preprocTaskStart(event: SparkListenerTaskStart): Unit = {}

  def preprocTaskEnd(event: SparkListenerTaskEnd): Unit = {}

  def preprocStageCompleted(event: SparkListenerStageCompleted): Unit = {}

  def preprocJobEnd(event: SparkListenerJobEnd): Unit = {}

  def preprocUnpersistRDD(event: SparkListenerUnpersistRDD): Unit = {}

  def preprocEndApp(event: SparkListenerApplicationEnd): Unit = {}


  // On events base no-ops

  def addApp(event: SparkListenerApplicationStart): Unit = {}

  def addExecutor(event: SparkListenerExecutorAdded): Unit = {}

  def removeExecutor(event: SparkListenerExecutorRemoved): Unit = {}

  def addBlockManager(event: SparkListenerBlockManagerAdded): Unit = {}

  def jobStart(event: SparkListenerJobStart): Unit = {}

  def stageSubmitted(event: SparkListenerStageSubmitted): Unit = {}

  def taskStart(event: SparkListenerTaskStart): Unit = {}

  def taskEnd(event: SparkListenerTaskEnd): Unit = {}

  def stageCompleted(event: SparkListenerStageCompleted): Unit = {}

  def jobEnd(event: SparkListenerJobEnd): Unit = {}

  def unpersistRDD(event: SparkListenerUnpersistRDD): Unit = {}

  def endApp(event: SparkListenerApplicationEnd): Unit = {}


  // Un event base no-ops

  def unAddApp(event: SparkListenerApplicationStart): Unit = {}

  def unAddExecutor(event: SparkListenerExecutorAdded): Unit = {}

  def unRemoveExecutor(event: SparkListenerExecutorRemoved): Unit = {}

  def unAddBlockManager(event: SparkListenerBlockManagerAdded): Unit = {}

  def unJobStart(event: SparkListenerJobStart): Unit = {}

  def unStageSubmitted(event: SparkListenerStageSubmitted): Unit = {}

  def unTaskStart(event: SparkListenerTaskStart): Unit = {}

  def unTaskEnd(event: SparkListenerTaskEnd): Unit = {}

  def unStageCompleted(event: SparkListenerStageCompleted): Unit = {}

  def unJobEnd(event: SparkListenerJobEnd): Unit = {}

  def unUnpersistRDD(event: SparkListenerUnpersistRDD): Unit = {}

  def unEndApp(event: SparkListenerApplicationEnd): Unit = {}

  // Main event routers

  final def preprocess(event: SparkListenerEvent): Unit = synchronized {
    onPreprocEvent(event)
    event match {
      case event: SparkListenerApplicationStart  => preprocAddApp(event)
      case event: SparkListenerExecutorAdded     => preprocAddExecutor(event)
      case event: SparkListenerExecutorRemoved   => preprocRemoveExecutor(event)
      case event: SparkListenerBlockManagerAdded => preprocAddBlockManager(event)
      case event: SparkListenerJobStart          => preprocJobStart(event)
      case event: SparkListenerStageSubmitted    => preprocStageSubmitted(event)
      case event: SparkListenerTaskStart         => preprocTaskStart(event)
      case event: SparkListenerTaskEnd           => preprocTaskEnd(event)
      case event: SparkListenerStageCompleted    => preprocStageCompleted(event)
      case event: SparkListenerJobEnd            => preprocJobEnd(event)
      case event: SparkListenerUnpersistRDD      => preprocUnpersistRDD(event)
      case event: SparkListenerApplicationEnd    => preprocEndApp(event)
      case _                                     =>
    }
  }

 final def onEvent(event: SparkListenerEvent): Unit = synchronized {
   onOnEvent(event)
   event match {
      case event: SparkListenerApplicationStart  => addApp(event)
      case event: SparkListenerExecutorAdded     => addExecutor(event)
      case event: SparkListenerExecutorRemoved   => removeExecutor(event)
      case event: SparkListenerBlockManagerAdded => addBlockManager(event)
      case event: SparkListenerJobStart          => jobStart(event)
      case event: SparkListenerStageSubmitted    => stageSubmitted(event)
      case event: SparkListenerTaskStart         => taskStart(event)
      case event: SparkListenerTaskEnd           => taskEnd(event)
      case event: SparkListenerStageCompleted    => stageCompleted(event)
      case event: SparkListenerJobEnd            => jobEnd(event)
      case event: SparkListenerUnpersistRDD      => unpersistRDD(event)
      case event: SparkListenerApplicationEnd    => endApp(event)
      case _                                     =>
    }
  }

  final def unEvent(event: SparkListenerEvent): Unit = synchronized {
    onUnEvent(event)
    event match {
      case event: SparkListenerApplicationStart  => unAddApp(event)
      case event: SparkListenerExecutorAdded     => unAddExecutor(event)
      case event: SparkListenerExecutorRemoved   => unRemoveExecutor(event)
      case event: SparkListenerBlockManagerAdded => unAddBlockManager(event)
      case event: SparkListenerJobStart          => unJobStart(event)
      case event: SparkListenerStageSubmitted    => unStageSubmitted(event)
      case event: SparkListenerTaskStart         => unTaskStart(event)
      case event: SparkListenerTaskEnd           => unTaskEnd(event)
      case event: SparkListenerStageCompleted    => unStageCompleted(event)
      case event: SparkListenerJobEnd            => unJobEnd(event)
      case event: SparkListenerUnpersistRDD      => unUnpersistRDD(event)
      case event: SparkListenerApplicationEnd    => unEndApp(event)
      case _                                     =>
    }
  }
}
