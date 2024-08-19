/*
 * Copyright 2023-2024 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Message
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Future
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit.MILLISECONDS
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

open class MessageBatcher(
    private val maxBatchSize: Int = 1000,
    private val maxFlushTime: Long = 1000,
    protected val book: String,
    private val batchSelector: (Message.Builder<*>, String) -> Any = GROUP_SELECTOR,
    private val executor: ScheduledExecutorService,
    private val onError: (Throwable) -> Unit = {},
    private val onBatch: (GroupBatch) -> Unit,
) : AutoCloseable {
    private val batches = ConcurrentHashMap<Any, Batch>()

    /**
     * Adds [message] to the batch for specified [sessionGroup].
     * Method is also updates the [Message.id] by setting `timestamp` to the current timestamp
     */
    open fun onMessage(message: Message.Builder<*>, sessionGroup: String) {
        batches.getOrPut(batchSelector(message, sessionGroup)) { newBatch(sessionGroup) }
            .add(message)
    }

    override fun close() {
        batches.values.forEach {
            it.close()
        }
    }

    protected open fun newBatch(sessionGroup: String): Batch = Batch(book, sessionGroup)

    open inner class Batch(
        book: String,
        group: String,
    ) : AutoCloseable {
        private val newBatch: () -> GroupBatch.Builder = {
            GroupBatch.builder().apply {
                setBook(book)
                setSessionGroup(group)
            }
        }

        private val lock = ReentrantLock()
        private var batch = newBatch()
        private var future: Future<*> = CompletableFuture.completedFuture(null)

        fun add(builder: Message.Builder<*>) = lock.withLock {
            builder.idBuilder().setTimestamp(Instant.now())
            val message: Message<*> = builder.build()

            beforeAdd(message)
            batch.addGroup(message.toGroup())
            afterAdd()

            when (batch.groupsBuilder().size) {
                // The order of check is important
                // In case of maxBatchSize = 1 we should not schedule any tasks
                // and just need to publish batch right away
                maxBatchSize -> send()
                1 -> future = executor.schedule(::send, maxFlushTime, MILLISECONDS)
            }
        }

        protected open fun beforeAdd(message: Message<*>) {}
        protected open fun afterAdd() {}
        protected open fun onNewBatch() {}

        protected fun send() = lock.withLock<Unit> {
            if (batch.groupsBuilder().isEmpty()) return
            runCatching { onBatch(batch.build()) }.onFailure(onError)
            batch = newBatch()
            onNewBatch()
            future.cancel(false)
        }

        override fun close() = send()
    }

    companion object {
        @JvmField
        val GROUP_SELECTOR: (Message.Builder<*>, String) -> Any = { _, group -> group }

        @JvmField
        val ALIAS_SELECTOR: (Message.Builder<*>, String) -> Any = { message, _ ->
            message.idBuilder().sessionAlias to message.idBuilder().direction
        }
    }
}

class RawMessageBatcher(
    private val maxBatchSizeInBytes: Long = 1_024 * 1_024,
    maxBatchSize: Int = 1000,
    maxFlushTime: Long = 1000,
    book: String,
    batchSelector: (Message.Builder<*>, String) -> Any = GROUP_SELECTOR,
    executor: ScheduledExecutorService,
    onError: (Throwable) -> Unit = {},
    onBatch: (GroupBatch) -> Unit,
) : MessageBatcher(
    maxBatchSize,
    maxFlushTime,
    book,
    batchSelector,
    executor,
    onError,
    onBatch,
) {

    /**
     * Adds [message] to the batch for specified [sessionGroup].
     * Method is also updates the [Message.id] by setting `timestamp` to the current timestamp
     */
    fun onMessage(message: RawMessage.Builder, sessionGroup: String) {
        super.onMessage(message, sessionGroup)
    }

    /**
     * Adds [message] to the batch for specified [sessionGroup].
     * Method is also updates the [Message.id] by setting `timestamp` to the current timestamp
     */
    override fun onMessage(message: Message.Builder<*>, sessionGroup: String) {
        require(message is RawMessage.Builder) {
            "${RawMessageBatcher::class.java.simpleName} handles only ${RawMessage.Builder::class.java.simpleName} " +
                    "but receive ${message::class.java.simpleName}"
        }
        onMessage(message, sessionGroup)
    }

    override fun newBatch(sessionGroup: String): Batch = RawBatch(book, sessionGroup)

    inner class RawBatch(
        book: String,
        group: String,
    ) : MessageBatcher.Batch(
        book,
        group,
    ) {
        private var batchSizeInBytes = CRADLE_BATCH_LEN_CONST

        override fun beforeAdd(message: Message<*>) {
            require(message is RawMessage) {
                "${RawBatch::class.java.simpleName} handles only ${RawMessage::class.java.simpleName} " +
                        "but receive ${message::class.java.simpleName}"
            }
            val messageSizeInBytes = message.calculateSizeInBytes() + CRADLE_RAW_MESSAGE_LENGTH_IN_BATCH
            if (batchSizeInBytes + messageSizeInBytes > maxBatchSizeInBytes) {
                send()
            } else {
                batchSizeInBytes += messageSizeInBytes
            }
        }

        override fun afterAdd() {
            if (batchSizeInBytes >= maxBatchSizeInBytes) {
                send()
            }
        }

        override fun onNewBatch() {
            super.onNewBatch()
            batchSizeInBytes = CRADLE_BATCH_LEN_CONST
        }
    }

    companion object {
        /**
         * Cradle raw message batch length constant
         * 4 - magic number
         * 1 - protocol version
         * 4 - message sizes
         * Collapsed constant = 9
         */
        internal const val CRADLE_BATCH_LEN_CONST = 9L

        /**
         * Cradle raw message length in batch constant
         * every message:
         * 4 - message length
         * x - message
         */
        internal const val CRADLE_RAW_MESSAGE_LENGTH_IN_BATCH = 4L

        /**
         * Cradle raw message size in batch constant
         * 2 - magic number
         * 8 - index (long)
         * 4 + 8 = Instant (timestamp) long (seconds) + int (nanos)
         * 4 - message body (byte[]) length
         * 4 - metadata (map) length
         *
         * Collapsed constant = 30
         *
         */
        internal const val CRADLE_RAW_MESSAGE_SIZE = 30L

        /**
         * Cradle direction size
         */
        internal const val CRADLE_DIRECTION_SIZE = 1L

        private fun RawMessage.calculateSizeInBytes(): Long = body.readableBytes() + CRADLE_RAW_MESSAGE_SIZE +
                id.sessionAlias.length +
                CRADLE_DIRECTION_SIZE +
                protocol.length +
                metadata.asSequence().sumOf { (key, value) -> key.length + value.length }
    }
}