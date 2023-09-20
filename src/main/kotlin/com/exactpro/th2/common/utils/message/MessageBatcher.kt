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

import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.sessionGroup
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Future
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit.MILLISECONDS
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

val RAW_DIRECTION_SELECTOR: (RawMessage.Builder) -> Any = { it.sessionAlias to it.direction }
val DIRECTION_SELECTOR: (Message.Builder) -> Any = { it.sessionAlias to it.direction }

val RAW_GROUP_SELECTOR: (RawMessage.Builder) -> Any = { it.sessionGroup }
val GROUP_SELECTOR: (Message.Builder) -> Any = { it.sessionGroup }

class RawMessageBatcher(
    maxBatchSize: Int = 1000,
    maxFlushTime: Long = 1000,
    private val batchSelector: (RawMessage.Builder) -> Any,
    executor: ScheduledExecutorService,
    onError: (Throwable) -> Unit = {},
    onBatch: (MessageGroupBatch) -> Unit
) : Batcher<RawMessage.Builder>(maxBatchSize, maxFlushTime, executor, onError, onBatch) {
    /**
     * Adds [message] to the batch selected by [batchSelector].
     * Updates timestamp in the [message] ID
     */
    override fun onMessage(message: RawMessage.Builder) {
        add(batchSelector(message), message)
    }
}

class MessageBatcher(
    maxBatchSize: Int = 1000,
    maxFlushTime: Long = 1000,
    private val batchSelector: (Message.Builder) -> Any,
    executor: ScheduledExecutorService,
    onError: (Throwable) -> Unit = {},
    onBatch: (MessageGroupBatch) -> Unit
) : Batcher<Message.Builder>(maxBatchSize, maxFlushTime, executor, onError, onBatch) {
    /**
     * Adds [message] to the batch selected by [batchSelector].
     * Updates timestamp in the [message] ID
     */
    override fun onMessage(message: Message.Builder) {
        add(batchSelector(message), message)
    }
}

abstract class Batcher<T>(
    private val maxBatchSize: Int = 1000,
    private val maxFlushTime: Long = 1000,
    private val executor: ScheduledExecutorService,
    private val onError: (Throwable) -> Unit = {},
    private val onBatch: (MessageGroupBatch) -> Unit
) : AutoCloseable {
    private val batches = ConcurrentHashMap<Any, Batch>()

    abstract fun onMessage(message: T)

    protected fun add(key: Any, message: RawMessage.Builder) = batches.getOrPut(key, ::Batch).add {
        message.metadataBuilder.idBuilder.timestamp = Instant.now().toTimestamp()
        message.toGroup()
    }
    protected fun add(key: Any, message: Message.Builder) = batches.getOrPut(key, ::Batch).add {
        message.metadataBuilder.idBuilder.timestamp = Instant.now().toTimestamp()
        message.toGroup()
    }
    protected fun add(key: Any, message: AnyMessage) = batches.getOrPut(key, ::Batch).add { message.toGroup() }
    protected fun add(key: Any, group: MessageGroup) = batches.getOrPut(key, ::Batch).add { group }

    override fun close() {
        batches.values.forEach {
            it.close()
        }
    }

    protected inner class Batch : AutoCloseable {
        private val lock = ReentrantLock()
        private var batch = MessageGroupBatch.newBuilder()
        private var future: Future<*> = CompletableFuture.completedFuture(null)

        fun add(groupSupplier: () -> MessageGroup) = lock.withLock {
            batch.addGroups(groupSupplier.invoke())

            when (batch.groupsCount) {
                1 -> future = executor.schedule(::send, maxFlushTime, MILLISECONDS)
                maxBatchSize -> send()
            }
        }

        private fun send() = lock.withLock<Unit> {
            if (batch.groupsCount == 0) return
            runCatching { onBatch(batch.build()) }.onFailure(onError)
            batch.clearGroups()
            future.cancel(false)
        }

        override fun close() = send()
    }
}
