package com.groupon.sparklint.data

import org.apache.spark.util.StatCounter

/**
  * @author rxue
  * @since 8/16/16.
  */
class CompressedTaskCounter(_outputMetrics: SparklintOutputCounter = new SparklintOutputCounter(),
                            _inputMetrics: SparklintInputCounter = new SparklintInputCounter(),
                            _shuffleReadMetrics: SparklintShuffleReadCounter = new SparklintShuffleReadCounter(),
                            _shuffleWriteMetrics: SparklintShuffleWriteCounter = new SparklintShuffleWriteCounter(),
                            _diskBytesSpilled: StatCounter = StatCounter(),
                            _memoryBytesSpilled: StatCounter = StatCounter(),
                            _executorDeserializeTime: StatCounter = StatCounter(),
                            _jvmGCTime: StatCounter = StatCounter(),
                            _resultSerializationTime: StatCounter = StatCounter(),
                            _resultSize: StatCounter = StatCounter(),
                            _executorRunTime: StatCounter = StatCounter()
                           ) extends SparklintTaskCounter {
  override def outputMetrics: SparklintOutputCounter = _outputMetrics

  override def inputMetrics: SparklintInputCounter = _inputMetrics

  override def shuffleReadMetrics: SparklintShuffleReadCounter = _shuffleReadMetrics

  override def shuffleWriteMetrics: SparklintShuffleWriteCounter = _shuffleWriteMetrics

  override def diskBytesSpilled: StatCounter = _diskBytesSpilled.copy()

  override def memoryBytesSpilled: StatCounter = _memoryBytesSpilled.copy()

  override def executorDeserializeTime: StatCounter = _executorDeserializeTime.copy()

  override def jvmGCTime: StatCounter = _jvmGCTime.copy()

  override def resultSerializationTime: StatCounter = _resultSerializationTime.copy()

  override def resultSize: StatCounter = _resultSize.copy()

  override def executorRunTime: StatCounter = _executorRunTime.copy()

  def merge(taskId: Long, metrics: SparklintTaskMetrics): CompressedTaskCounter = {
    _inputMetrics.merge(metrics.inputMetrics)
    _outputMetrics.merge(metrics.outputMetrics)
    _shuffleReadMetrics.merge(metrics.shuffleReadMetrics)
    _shuffleWriteMetrics.merge(metrics.shuffleWriteMetrics)
    _diskBytesSpilled.merge(metrics.diskBytesSpilled)
    _memoryBytesSpilled.merge(metrics.memoryBytesSpilled)
    _executorDeserializeTime.merge(metrics.executorDeserializeTime)
    _jvmGCTime.merge(metrics.jvmGCTime)
    _resultSerializationTime.merge(metrics.resultSerializationTime)
    _resultSize.merge(metrics.resultSize)
    _executorRunTime.merge(metrics.executorRunTime)
    this
  }
}
