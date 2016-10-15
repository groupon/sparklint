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
package com.groupon.sparklint

import com.groupon.sparklint.events.{EventSourceLike, FreeScrollEventSource}
import org.apache.spark.scheduler.{SparkListenerEvent, SparkListenerStageSubmitted, StageInfo}

/**
  * @author rxue
  * @since 8/19/16.
  */
object TestUtils {

  def resource(name: String): String = {
    ResourceHelper.convertResourcePathToFilePath(getClass.getClassLoader, name)
  }

  def replay(eventSource: EventSourceLike with FreeScrollEventSource, count: Long = Long.MaxValue) = {
    var counter = 0
    var progress = eventSource.progress
    while (counter < count && progress.hasNext) {
      progress = eventSource.forwardEvents()
      counter += 1
    }
  }

  def sparkStageSubmittedEvent(id: Int, name: String): SparkListenerEvent = {
    SparkListenerStageSubmitted(new StageInfo(id, 42, name, 42, Seq.empty, Seq.empty, "details"))
  }
}


