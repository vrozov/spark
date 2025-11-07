/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.streaming.kinesis

import scala.collection.concurrent.TrieMap
import scala.util.control.NonFatal

import software.amazon.kinesis.processor.RecordProcessorCheckpointer

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{SHARD_ID, WORKER_URL}
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.util.RecurringTimer
import org.apache.spark.util.{Clock, SystemClock}

/**
 * This is a helper class for managing Kinesis checkpointing.
 *
 * @param receiver The receiver that keeps track of which sequence numbers we can checkpoint
 * @param checkpointInterval How frequently we will checkpoint to DynamoDB
 * @param schedulerId Worker Id of KCL worker for logging purposes
 * @param clock In order to use ManualClocks for the purpose of testing
 */
private[kinesis] class KinesisCheckpointer(
    receiver: KinesisReceiver[_],
    checkpointInterval: Duration,
    schedulerId: String,
    clock: Clock = new SystemClock) extends Logging {

  // a map from shardId's to checkpointers
  private val checkpointers = TrieMap.empty[String, RecordProcessorCheckpointer]

  private val lastCheckpointedSeqNums = TrieMap.empty[String, String]

  private val checkpointerTimer: RecurringTimer = startCheckpointerTimer()

  /** Update the checkpointer instance to the most recent one for the given shardId. */
  def setCheckpointer(shardId: String, checkpointer: RecordProcessorCheckpointer): Unit = {
    checkpointers.put(shardId, checkpointer)
  }

  /**
   * Stop tracking the specified shardId.
   *
   * If a checkpointer is provided, e.g. on ShardRecordProcessor.shutdown [[ShutdownReason.TERMINATE]],
   * we will use that to make the final checkpoint. If `None` is provided, we will not make the
   * checkpoint, e.g. in case of [[ShutdownReason.ZOMBIE]].
   */
  def removeCheckpointer(
      shardId: String,
      checkpointer: RecordProcessorCheckpointer): Unit = {
    checkpointers.remove(shardId)
    try {
      // We must call `checkpoint()` with no parameter to finish reading shards.
      // See a URL below for details:
      // https://forums.aws.amazon.com/thread.jspa?threadID=244218
      KinesisShardRecordProcessor.retryRandom(checkpointer.checkpoint(), 4, 100)
    } catch {
      case NonFatal(e) =>
        logError(log"Exception: Scheduler ${MDC(WORKER_URL, schedulerId)} encountered an " +
            log"exception while checkpointing to finish reading a shard of " +
            log"${MDC(SHARD_ID, shardId)}.", e)
        // Rethrow the exception to the Kinesis Worker that is managing this RecordProcessor
        throw e
    }
  }

  /** Perform the checkpoint. */
  private def checkpoint(
      shardId: String,
      checkpointer: RecordProcessorCheckpointer): Unit = {
    receiver.getLatestSeqNumToCheckpoint(shardId).foreach { latestSeqNum =>
      val lastSeqNum = lastCheckpointedSeqNums.get(shardId)
      // Kinesis sequence numbers are monotonically increasing strings, therefore we can
      // safely do the string comparison
      if (lastSeqNum.getOrElse("") < latestSeqNum) {
        try {
          /* Perform the checkpoint */
          KinesisShardRecordProcessor.retryRandom(checkpointer.checkpoint(latestSeqNum), 4, 100)
          logDebug(s"Checkpoint:  SchedulerId $schedulerId completed checkpoint " +
            s"at sequence number $latestSeqNum for shardId $shardId")
          lastCheckpointedSeqNums.put(shardId, latestSeqNum)
        } catch {
          case NonFatal(e) =>
            logWarning(log"Failed to checkpoint shardId ${MDC(SHARD_ID, shardId)} to DynamoDB.", e)
        }
      }
    }
  }

  /** Checkpoint the latest saved sequence numbers for all active shardId's. */
  private def checkpointAll(): Unit = synchronized {
    // if this method throws an exception, then the scheduled task will not run again
    try {
      checkpointers.foreach(c => checkpoint(c._1, c._2))
    } catch {
      case NonFatal(e) =>
        logWarning("Failed to checkpoint to DynamoDB.", e)
    }
  }

  /**
   * Start the checkpointer Timer with the given checkpoint duration.
   */
  private def startCheckpointerTimer(): RecurringTimer = {
    val period = checkpointInterval.milliseconds
    val name = s"Kinesis Checkpointer - Scheduler $schedulerId"
    val timer = new RecurringTimer(clock, period, _ => checkpointAll(), name)
    val start = timer.start()
    timer
  }

  /**
   * Shutdown the checkpointer. Should be called on the onStop of the Receiver.
   */
  def shutdown(): Unit = {
    // the recurring timer checkpoints for us one last time.
    checkpointerTimer.stop(interruptTimer = false)
    checkpointers.clear()
    lastCheckpointedSeqNums.clear()
    logInfo("Successfully shutdown Kinesis Checkpointer.")
  }
}
