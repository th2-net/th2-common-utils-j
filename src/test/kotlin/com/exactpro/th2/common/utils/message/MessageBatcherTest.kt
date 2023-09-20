/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.utils.message

import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.sequence
import com.exactpro.th2.common.utils.shutdownGracefully
import com.google.protobuf.Timestamp
import com.google.protobuf.util.Timestamps
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.RepeatedTest
import org.junit.jupiter.api.TestInstance
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.timeout
import org.mockito.kotlin.verify
import java.util.concurrent.Executors

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class MessageBatcherTest {
    private val callExecutor = Executors.newCachedThreadPool()
    private val executor = Executors.newSingleThreadScheduledExecutor()

    @AfterAll
    internal fun shutdown() {
        executor.shutdownGracefully()
    }

    @RepeatedTest(value = 100)
    fun `concurrent invocations preserve order by timestamp in parsed batch`() {
        val onBatch = mock<(MessageGroupBatch) -> Unit> { }
        val batcher = MessageBatcher(
            maxBatchSize = 10,
            maxFlushTime = 100,
            batchSelector = GROUP_SELECTOR,
            onBatch = onBatch,
            onError = {},
            executor = executor,
        )
        repeat(10) {
            callExecutor.submit {
                batcher.onMessage(
                    Message.newBuilder()
                        .apply {
                            sessionGroup = "test_group"
                            sessionAlias = "test"
                            sequence = it.toLong()
                            direction = Direction.FIRST
                        }
                )
            }
        }

        val batch = argumentCaptor<MessageGroupBatch>()
        verify(onBatch, timeout(1000).atLeastOnce()).invoke(batch.capture())
        val groupBatch = batch.firstValue
        Assertions.assertTrue(groupBatch.groupsCount > 0) { "empty group batch" }
        val messageSequence = groupBatch.groupsList.asSequence()
            .flatMap { it.messagesList }
        var prevTimestamp: Timestamp? = null
        for (message in messageSequence) {
            val timestamp = message.id.timestamp
            if (prevTimestamp != null) {
                Assertions.assertTrue(
                    prevTimestamp <= timestamp,
                ) {
                    "unordered timestamps: ${Timestamps.toString(prevTimestamp)}, ${Timestamps.toString(timestamp)}"
                }
            }
            prevTimestamp = timestamp
        }
    }

    @RepeatedTest(value = 100)
    fun `concurrent invocations preserve order by timestamp in raw batch`() {
        val onBatch = mock<(MessageGroupBatch) -> Unit> { }
        val batcher = RawMessageBatcher(
            maxBatchSize = 10,
            maxFlushTime = 100,
            batchSelector = RAW_GROUP_SELECTOR,
            onBatch = onBatch,
            onError = {},
            executor = executor,
        )
        repeat(10) {
            callExecutor.submit {
                batcher.onMessage(
                    RawMessage.newBuilder()
                        .apply {
                            sessionGroup = "test_group"
                            sessionAlias = "test"
                            sequence = it.toLong()
                            direction = Direction.FIRST
                        }
                )
            }
        }

        val batch = argumentCaptor<MessageGroupBatch>()
        verify(onBatch, timeout(1000).atLeastOnce()).invoke(batch.capture())
        val groupBatch = batch.firstValue
        Assertions.assertTrue(groupBatch.groupsCount > 0) { "empty group batch" }
        val messageSequence = groupBatch.groupsList.asSequence()
            .flatMap { it.messagesList }
        var prevTimestamp: Timestamp? = null
        for (message in messageSequence) {
            val timestamp = message.id.timestamp
            if (prevTimestamp != null) {
                Assertions.assertTrue(
                    prevTimestamp <= timestamp,
                ) {
                    "unordered timestamps: ${Timestamps.toString(prevTimestamp)}, ${Timestamps.toString(timestamp)}"
                }
            }
            prevTimestamp = timestamp
        }
    }
}

private operator fun Timestamp.compareTo(timestamp: Timestamp): Int = Timestamps.compare(this, timestamp)
