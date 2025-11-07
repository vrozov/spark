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

import scala.jdk.CollectionConverters._
import scala.util.Random
import scala.util.control.NonFatal

import software.amazon.kinesis.exceptions.{InvalidStateException, KinesisClientLibDependencyException, ShutdownException, ThrottlingException}
import software.amazon.kinesis.lifecycle.events.{InitializationInput, LeaseLostInput, ProcessRecordsInput, ShardEndedInput, ShutdownRequestedInput}
import software.amazon.kinesis.processor.ShardRecordProcessor

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{RETRY_INTERVAL, SHARD_ID, WORKER_URL}

/**
 * Kinesis-specific implementation of the Kinesis Client Library (KCL) IRecordProcessor.
 * This implementation operates on the Array[Byte] from the KinesisReceiver.
 * The Kinesis Worker creates an instance of this KinesisRecordProcessor for each
 * shard in the Kinesis stream upon startup.  This is normally done in separate threads,
 * but the KCLs within the KinesisReceivers will balance themselves out if you create
 * multiple Receivers.
 *
 * @param receiver Kinesis receiver
 * @param schedulerId for logging purposes
 */
private[kinesis] class KinesisShardRecordProcessor[T](
    receiver: KinesisReceiver[T],
    schedulerId: String) extends ShardRecordProcessor with Logging {

  // shardId populated during initialize()
  @volatile private var shardId: String = _

  /**
   * The Kinesis Client Library calls this method during ShardRecordProcessor initialization.
   *
   * @param initializationInput Provides information related to initialization
   */
  override def initialize(initializationInput: InitializationInput): Unit = {
    shardId = initializationInput.shardId()
    logInfo(log"Initialized schedulerId ${MDC(WORKER_URL, schedulerId)} " +
      log"with shardId ${MDC(SHARD_ID, shardId)}")
  }

  /**
   * Process data records. The Amazon Kinesis Client Library will invoke this method to deliver
   * data records to the application.
   * Upon fail over, the new instance will get records with sequence number > checkpoint position
   * for each partition key.
   *
   * @param processRecordsInput Provides the records to be processed as well as information and
   *                            capabilities related to them (eg checkpointing).
   *
   */
  override def processRecords(processRecordsInput: ProcessRecordsInput): Unit = {
    if (!receiver.isStopped()) {
      try {
        // Limit the number of processed records from Kinesis stream. This is because the KCL cannot
        // control the number of aggregated records to be fetched even if we set `MaxRecords`
        // in `KinesisClientLibConfiguration`. For example, if we set 10 to the number of max
        // records in a worker and a producer aggregates two records into one message, the worker
        // possibly 20 records every callback function called.
        val limit = receiver.getCurrentLimit
        val records = processRecordsInput.records().asScala
        for (start <- 0 to records.size by limit) {
          val size = math.min(limit, records.size - start)
          val miniBatch = records.slice(start, start + size).toSeq
          receiver.addRecords(shardId, miniBatch)
          logDebug(s"Stored: scheduler $schedulerId stored ${miniBatch.size} records " +
            s"for shardId $shardId")
        }
        receiver.setCheckpointer(shardId, processRecordsInput.checkpointer)
      } catch {
        case NonFatal(e) =>
          /*
           *  If there is a failure within the batch, the batch will not be checkpointed.
           *  This will potentially cause records since the last checkpoint to be processed
           *  more than once.
           */
          logError(log"Exception: scheduler ${MDC(WORKER_URL, schedulerId)} encountered " +
            log"exception while storing or checkpointing a batch for schedulerId " +
            log"${MDC(WORKER_URL, schedulerId)} and shardId ${MDC(SHARD_ID, shardId)}.", e)

          /* Rethrow the exception to the Kinesis Worker that is managing this RecordProcessor. */
          throw e
      }
    } else {
      /* RecordProcessor has been stopped. */
      logInfo(log"Stopped: KinesisReceiver has stopped for scheduler ${MDC(WORKER_URL, schedulerId)}" +
          log" and shardId ${MDC(SHARD_ID, shardId)}. No more records will be processed.")
    }
  }

  /**
   * Kinesis Client Library is shutting down this Worker for 1 of 2 reasons:
   * 1) the stream is resharding by splitting or merging adjacent shards
   *     (ShutdownReason.TERMINATE)
   * 2) the failed or latent Worker has stopped sending heartbeats for whatever reason
   *     (ShutdownReason.ZOMBIE)
   *
   * @param checkpointer used to perform a Kinesis checkpoint for ShutdownReason.TERMINATE
   * @param reason for shutdown (ShutdownReason.TERMINATE or ShutdownReason.ZOMBIE)
   */
  override def shutdownRequested(
      shutdownRequestedInput: ShutdownRequestedInput): Unit = {
    logInfo(log"Shutdown: Shutting down scheduler ${MDC(WORKER_URL, schedulerId)}")
    // null if not initialized before shutdown:
    if (shardId == null) {
      logWarning(log"No shardId for workerId ${MDC(WORKER_URL, schedulerId)}?")
    } else {
      receiver.removeCheckpointer(shardId, shutdownRequestedInput.checkpointer)
    }
  }

  override def leaseLost(leaseLostInput: LeaseLostInput): Unit = {

  }

  override def shardEnded(shardEndedInput: ShardEndedInput): Unit = {

  }
}

private[kinesis] object KinesisShardRecordProcessor extends Logging {
  /**
   * Retry the given amount of times with a random backoff time (millis) less than the
   *   given maxBackOffMillis
   *
   * @param expression expression to evaluate
   * @param numRetriesLeft number of retries left
   * @param maxBackOffMillis: max millis between retries
   *
   * @return evaluation of the given expression
   * @throws Unretryable exception, unexpected exception,
   *  or any exception that persists after numRetriesLeft reaches 0
   */
  @annotation.tailrec
  def retryRandom[T](expression: => T, numRetriesLeft: Int, maxBackOffMillis: Int): T = {
    util.Try { expression } match {
      /* If the function succeeded, evaluate to x. */
      case util.Success(x) => x
      /* If the function failed, either retry or throw the exception */
      case util.Failure(e) => e match {
        /* Retry:  Throttling or other Retryable exception has occurred */
        case _: ThrottlingException | _: KinesisClientLibDependencyException
            if numRetriesLeft > 1 =>
          val backOffMillis = Random.nextInt(maxBackOffMillis)
          Thread.sleep(backOffMillis)
          logError(log"Retryable Exception: Random " +
            log"backOffMillis=${MDC(RETRY_INTERVAL, backOffMillis)}", e)
          retryRandom(expression, numRetriesLeft - 1, maxBackOffMillis)
        /* Throw:  Shutdown has been requested by the Kinesis Client Library. */
        case _: ShutdownException =>
          logError(s"ShutdownException: Caught shutdown exception, skipping checkpoint.", e)
          throw e
        /* Throw:  Non-retryable exception has occurred with the Kinesis Client Library */
        case _: InvalidStateException =>
          logError(s"InvalidStateException: Cannot save checkpoint to the DynamoDB table used" +
              s" by the Amazon Kinesis Client Library.  Table likely doesn't exist.", e)
          throw e
        /* Throw:  Unexpected exception has occurred */
        case _ =>
          logError(s"Unexpected, non-retryable exception.", e)
          throw e
      }
    }
  }
}
