/*
 * Copyright 2024 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.utils.message.transport

import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Message
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage
import com.exactpro.th2.common.utils.message.transport.RawMessageBatcher.Companion.CRADLE_BATCH_LEN_CONST
import com.exactpro.th2.common.utils.message.transport.RawMessageBatcher.Companion.CRADLE_DIRECTION_SIZE
import com.exactpro.th2.common.utils.message.transport.RawMessageBatcher.Companion.CRADLE_RAW_MESSAGE_LENGTH_IN_BATCH
import com.exactpro.th2.common.utils.message.transport.RawMessageBatcher.Companion.CRADLE_RAW_MESSAGE_SIZE
import com.exactpro.th2.common.utils.shutdownGracefully
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.RepeatedTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertThrows
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.timeout
import org.mockito.kotlin.verify
import java.time.Instant
import java.util.concurrent.Executors
import kotlin.text.Charsets.UTF_8

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class RawMessageBatcherTest {
    private val callExecutor = Executors.newCachedThreadPool()
    private val executor = Executors.newSingleThreadScheduledExecutor()

    @AfterAll
    internal fun shutdown() {
        executor.shutdownGracefully()
        callExecutor.shutdownGracefully()
    }

    @RepeatedTest(value = 100)
    fun `concurrent invocations preserve order by timestamp in batch`() {
        val onBatch = mock<(GroupBatch) -> Unit> { }
        val batcher = RawMessageBatcher(
            maxBatchSizeInBytes = 1024 * 1024,
            maxBatchSize = 10,
            maxFlushTime = 100,
            book = "test",
            onBatch = onBatch,
            onError = {},
            executor = executor,
        )
        repeat(10) {
            callExecutor.submit {
                batcher.onMessage(
                    RawMessage.builder().apply {
                        idBuilder().setSessionAlias("test")
                            .setDirection(Direction.INCOMING)
                            .setSequence(it.toLong())
                    },
                    "test_group"
                )
            }
        }

        val batch = argumentCaptor<GroupBatch>()
        verify(onBatch, timeout(1_000).atLeastOnce()).invoke(batch.capture())
        val groupBatch = batch.allValues.single()

        assertTrue(groupBatch.groups.isNotEmpty()) { "empty group batch" }
        val messageSequence = groupBatch.groups.asSequence()
            .flatMap { it.messages }
        var prevTimestamp: Instant? = null
        for (message in messageSequence) {
            val timestamp = message.id.timestamp
            if (prevTimestamp != null) {
                assertTrue(
                    prevTimestamp <= timestamp,
                ) {
                    "unordered timestamps: $prevTimestamp, $timestamp"
                }
            }
            prevTimestamp = timestamp
        }
    }

    @Test
    fun `send batch with single message when limit is 1 message`() {
        val onBatch = mock<(GroupBatch) -> Unit> { }
        val batcher = RawMessageBatcher(
            maxBatchSizeInBytes = 1024 * 1024,
            maxBatchSize = 1,
            maxFlushTime = 1000,
            book = "test",
            onBatch = onBatch,
            onError = {},
            executor = executor,
        )

        batcher.onMessage(
            RawMessage.builder().apply {
                idBuilder().setSessionAlias("test")
                    .setDirection(Direction.INCOMING)
                    .setSequence(1L)
            },
            "test_group"
        )

        val argumentCaptor = argumentCaptor<GroupBatch>()
        verify(onBatch).invoke(argumentCaptor.capture())

        val batch: GroupBatch = argumentCaptor.firstValue
        assertEquals(1, batch.groups.size) { "unexpected number of groups" }
        val group = batch.groups.single()
        assertEquals(1, group.messages.size) { "unexpected number of messages" }
        val message: Message<*> = group.messages.single()
        assertEquals(
            MessageId.builder()
                .setTimestamp(message.id.timestamp)
                .setSessionAlias("test")
                .setDirection(Direction.INCOMING)
                .setSequence(1L)
                .build(),
            message.id,
            "unexpected message id",
        )
    }

    @Test
    fun `send batch with single message when limit is 1 byte`() {
        val onBatch = mock<(GroupBatch) -> Unit> { }
        val batcher = RawMessageBatcher(
            maxBatchSizeInBytes = 1,
            maxBatchSize = 10,
            maxFlushTime = 1000,
            book = "test",
            onBatch = onBatch,
            onError = {},
            executor = executor,
        )

        batcher.onMessage(
            RawMessage.builder().apply {
                idBuilder().setSessionAlias("test")
                    .setDirection(Direction.INCOMING)
                    .setSequence(1L)
            },
            "test_group"
        )

        val argumentCaptor = argumentCaptor<GroupBatch>()
        verify(onBatch).invoke(argumentCaptor.capture())

        val batch: GroupBatch = argumentCaptor.firstValue
        assertEquals(1, batch.groups.size) { "unexpected number of groups" }
        val group = batch.groups.single()
        assertEquals(1, group.messages.size) { "unexpected number of messages" }
        val message: Message<*> = group.messages.single()
        assertEquals(
            MessageId.builder()
                .setTimestamp(message.id.timestamp)
                .setSessionAlias("test")
                .setDirection(Direction.INCOMING)
                .setSequence(1L)
                .build(),
            message.id,
            "unexpected message id",
        )
    }

    @Test
    fun `send batch with single message when limit equals to message size`() {
        val onBatch = mock<(GroupBatch) -> Unit> { }
        val sessionAlias = "test-session-alias"
        val protocol = "test-protocol"
        val metadata = mapOf("test-property" to "test-property-value")
        val body = "test-body".toByteArray(UTF_8)
        val batcher = RawMessageBatcher(
            maxBatchSizeInBytes = CRADLE_BATCH_LEN_CONST +
                    CRADLE_RAW_MESSAGE_LENGTH_IN_BATCH +
                    CRADLE_RAW_MESSAGE_SIZE +
                    sessionAlias.length +
                    CRADLE_DIRECTION_SIZE +
                    protocol.length +
                    metadata.asSequence().sumOf { (key, value) -> key.length + value.length } +
                    body.size,
            maxBatchSize = 10,
            maxFlushTime = 1000,
            book = "test",
            onBatch = onBatch,
            onError = {},
            executor = executor,
        )

        batcher.onMessage(
            RawMessage.builder().apply {
                idBuilder().setSessionAlias(sessionAlias)
                    .setDirection(Direction.INCOMING)
                    .setSequence(1L)
                setProtocol(protocol)
                metadataBuilder().putAll(metadata)
                setBody(body)
            },
            "test-session-group"
        )

        val argumentCaptor = argumentCaptor<GroupBatch>()
        verify(onBatch).invoke(argumentCaptor.capture())

        val batch: GroupBatch = argumentCaptor.firstValue
        assertEquals(1, batch.groups.size) { "unexpected number of groups" }
        val group = batch.groups.single()
        assertEquals(1, group.messages.size) { "unexpected number of messages" }
        val message: Message<*> = group.messages.single()
        assertEquals(
            MessageId.builder()
                .setTimestamp(message.id.timestamp)
                .setSessionAlias(sessionAlias)
                .setDirection(Direction.INCOMING)
                .setSequence(1L)
                .build(),
            message.id,
            "unexpected message id",
        )
    }

    @Test
    fun `send parsed message`() {
        val onBatch = mock<(GroupBatch) -> Unit> { }
        val batcher = RawMessageBatcher(
            book = "test",
            onBatch = onBatch,
            executor = executor,
        )

        assertThrows<IllegalArgumentException> {
            batcher.onMessage(
                ParsedMessage.builder().apply {
                    idBuilder().setSessionAlias("test")
                        .setDirection(Direction.INCOMING)
                        .setSequence(1L)
                },
                "test_group"
            )
        }.also { exception ->
            assertEquals("RawMessageBatcher handles only Builder but receive FromMapBuilderImpl", exception.message)
        }
        verify(onBatch, never()).invoke(any())
    }
}