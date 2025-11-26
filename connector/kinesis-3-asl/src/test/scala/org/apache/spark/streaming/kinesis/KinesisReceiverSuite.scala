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

import java.util.Arrays

import scala.jdk.CollectionConverters._

import org.mockito.ArgumentMatchers.{anyList, anyString, eq => meq}
import org.mockito.Mockito.{never, times, verify, when}
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers._
import org.scalatestplus.mockito.MockitoSugar
import software.amazon.awssdk.core.SdkBytes
import software.amazon.kinesis.exceptions.{InvalidStateException, KinesisClientLibDependencyException, ShutdownException, ThrottlingException}
import software.amazon.kinesis.lifecycle.events.{InitializationInput, ProcessRecordsInput, ShutdownRequestedInput}
import software.amazon.kinesis.processor.RecordProcessorCheckpointer
import software.amazon.kinesis.retrieval.KinesisClientRecord

import org.apache.spark.streaming.{Duration, TestSuiteBase}

/**
 * Suite of Kinesis streaming receiver tests focusing mostly on the KinesisRecordProcessor
 */
class KinesisReceiverSuite
    extends TestSuiteBase
    with Matchers
    with BeforeAndAfter
    with MockitoSugar {

  val app = "TestKinesisReceiver"
  val stream = "mySparkStream"
  val endpoint = "endpoint-url"
  val schedulerId = "dummySchedulerId"
  val shardId = "dummyShardId"
  val seqNum = "dummySeqNum"
  val checkpointInterval = Duration(10)
  val someSeqNum = Some(seqNum)

  val record1 = KinesisClientRecord
    .builder()
    .data(SdkBytes.fromUtf8String("Spark In Action").asByteBuffer())
    .build()
  val record2 = KinesisClientRecord
    .builder()
    .data(SdkBytes.fromUtf8String("Learning Spark").asByteBuffer())
    .build()
  val batch = Arrays.asList(record1, record2)

  var receiverMock: KinesisReceiver[Array[Byte]] = _
  var checkpointerMock: RecordProcessorCheckpointer = _

  override def beforeFunction(): Unit = {
    receiverMock = mock[KinesisReceiver[Array[Byte]]]
    checkpointerMock = mock[RecordProcessorCheckpointer]
  }

  test("process records including store and set checkpointer") {
    when(receiverMock.isStopped()).thenReturn(false)
    when(receiverMock.getCurrentLimit).thenReturn(Int.MaxValue)

    val recordProcessor = new KinesisShardRecordProcessor(receiverMock, schedulerId)
    recordProcessor.initialize(InitializationInput.builder().shardId(shardId).build())
    recordProcessor.processRecords(
      ProcessRecordsInput
        .builder()
        .records(batch)
        .checkpointer(checkpointerMock)
        .build())

    verify(receiverMock, times(1)).isStopped()
    verify(receiverMock, times(1)).addRecords(shardId, batch.asScala.toSeq)
    verify(receiverMock, times(1)).setCheckpointer(shardId, checkpointerMock)
  }

  test("split into multiple processes if a limitation is set") {
    when(receiverMock.isStopped()).thenReturn(false)
    when(receiverMock.getCurrentLimit).thenReturn(1)

    val recordProcessor = new KinesisShardRecordProcessor(receiverMock, schedulerId)
    recordProcessor.initialize(InitializationInput.builder().shardId(shardId).build())
    recordProcessor.processRecords(
      ProcessRecordsInput
        .builder()
        .records(batch)
        .checkpointer(checkpointerMock)
        .build())

    verify(receiverMock, times(1)).isStopped()
    verify(receiverMock, times(1)).addRecords(shardId, batch.subList(0, 1).asScala.toSeq)
    verify(receiverMock, times(1)).addRecords(shardId, batch.subList(1, 2).asScala.toSeq)
    verify(receiverMock, times(1)).setCheckpointer(shardId, checkpointerMock)
  }

  test("shouldn't store and update checkpointer when receiver is stopped") {
    when(receiverMock.isStopped()).thenReturn(true)
    when(receiverMock.getCurrentLimit).thenReturn(Int.MaxValue)

    val recordProcessor = new KinesisShardRecordProcessor(receiverMock, schedulerId)
    recordProcessor.processRecords(
      ProcessRecordsInput
        .builder()
        .records(batch)
        .checkpointer(checkpointerMock)
        .build())

    verify(receiverMock, times(1)).isStopped()
    verify(receiverMock, never).addRecords(anyString, anyList().asScala.toSeq)
    verify(receiverMock, never).setCheckpointer(anyString, meq(checkpointerMock))
  }

  test("shouldn't update checkpointer when exception occurs during store") {
    when(receiverMock.isStopped()).thenReturn(false)
    when(receiverMock.getCurrentLimit).thenReturn(Int.MaxValue)
    when(receiverMock.addRecords(anyString, anyList().asScala.toSeq))
      .thenThrow(new RuntimeException())

    intercept[RuntimeException] {
      val recordProcessor = new KinesisShardRecordProcessor(receiverMock, schedulerId)
      recordProcessor.initialize(InitializationInput.builder().shardId(shardId).build())
      recordProcessor.processRecords(
        ProcessRecordsInput
          .builder()
          .records(batch)
          .checkpointer(checkpointerMock)
          .build())
    }

    verify(receiverMock, times(1)).isStopped()
    verify(receiverMock, times(1)).addRecords(shardId, batch.asScala.toSeq)
    verify(receiverMock, never).setCheckpointer(anyString, meq(checkpointerMock))
  }

  test("shutdown should checkpoint if the reason is TERMINATE") {
    when(receiverMock.getLatestSeqNumToCheckpoint(shardId)).thenReturn(someSeqNum)

    val recordProcessor = new KinesisShardRecordProcessor(receiverMock, schedulerId)
    recordProcessor.initialize(
      InitializationInput
        .builder()
        .shardId(shardId)
        .build())
    recordProcessor.shutdownRequested(
      ShutdownRequestedInput
        .builder()
        .checkpointer(checkpointerMock)
        .build())

    verify(receiverMock, times(1)).removeCheckpointer(meq(shardId), meq(checkpointerMock))
  }

  test("shutdown should not checkpoint if the reason is something other than TERMINATE") {
    when(receiverMock.getLatestSeqNumToCheckpoint(shardId)).thenReturn(someSeqNum)

    val recordProcessor = new KinesisShardRecordProcessor(receiverMock, schedulerId)
    recordProcessor.initialize(
      InitializationInput
        .builder()
        .shardId(shardId)
        .build())
    // recordProcessor.shutdown(checkpointerMock, ShutdownReason.ZOMBIE)
    // recordProcessor.shutdown(checkpointerMock, null)

    verify(receiverMock, times(2))
      .removeCheckpointer(meq(shardId), meq[RecordProcessorCheckpointer](null))
  }

  test("retry success on first attempt") {
    val expectedIsStopped = false
    when(receiverMock.isStopped()).thenReturn(expectedIsStopped)

    val actualVal = KinesisShardRecordProcessor.retryRandom(receiverMock.isStopped(), 2, 100)
    assert(actualVal == expectedIsStopped)

    verify(receiverMock, times(1)).isStopped()
  }

  test("retry success on second attempt after a Kinesis throttling exception") {
    val expectedIsStopped = false
    when(receiverMock.isStopped())
      .thenThrow(new ThrottlingException("error message"))
      .thenReturn(expectedIsStopped)

    val actualVal = KinesisShardRecordProcessor.retryRandom(receiverMock.isStopped(), 2, 100)
    assert(actualVal == expectedIsStopped)

    verify(receiverMock, times(2)).isStopped()
  }

  test("retry success on second attempt after a Kinesis dependency exception") {
    val expectedIsStopped = false
    when(receiverMock.isStopped())
      .thenThrow(new KinesisClientLibDependencyException("error message"))
      .thenReturn(expectedIsStopped)

    val actualVal = KinesisShardRecordProcessor.retryRandom(receiverMock.isStopped(), 2, 100)
    assert(actualVal == expectedIsStopped)

    verify(receiverMock, times(2)).isStopped()
  }

  test("retry failed after a shutdown exception") {
    when(checkpointerMock.checkpoint()).thenThrow(new ShutdownException("error message"))

    intercept[ShutdownException] {
      KinesisShardRecordProcessor.retryRandom(checkpointerMock.checkpoint(), 2, 100)
    }

    verify(checkpointerMock, times(1)).checkpoint()
  }

  test("retry failed after an invalid state exception") {
    when(checkpointerMock.checkpoint()).thenThrow(new InvalidStateException("error message"))

    intercept[InvalidStateException] {
      KinesisShardRecordProcessor.retryRandom(checkpointerMock.checkpoint(), 2, 100)
    }

    verify(checkpointerMock, times(1)).checkpoint()
  }

  test("retry failed after unexpected exception") {
    when(checkpointerMock.checkpoint()).thenThrow(new RuntimeException("error message"))

    intercept[RuntimeException] {
      KinesisShardRecordProcessor.retryRandom(checkpointerMock.checkpoint(), 2, 100)
    }

    verify(checkpointerMock, times(1)).checkpoint()
  }

  test("retry failed after exhausting all retries") {
    val expectedErrorMessage = "final try error message"
    when(checkpointerMock.checkpoint())
      .thenThrow(new ThrottlingException("error message"))
      .thenThrow(new ThrottlingException(expectedErrorMessage))

    val exception = intercept[RuntimeException] {
      KinesisShardRecordProcessor.retryRandom(checkpointerMock.checkpoint(), 2, 100)
    }
    exception.getMessage().shouldBe(expectedErrorMessage)

    verify(checkpointerMock, times(2)).checkpoint()
  }
}
