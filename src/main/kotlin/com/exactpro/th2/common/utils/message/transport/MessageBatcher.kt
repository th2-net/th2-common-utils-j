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

package com.exactpro.th2.common.utils.message.transport

import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Message
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Future
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit.MILLISECONDS
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class MessageBatcher(
    private val maxBatchSize: Int = 1000,
    private val maxFlushTime: Long = 1000,
    private val book: String,
    private val batchSelector: (Message.Builder<*>, String) -> Any = GROUP_SELECTOR,
    private val executor: ScheduledExecutorService,
    private val onError: (Throwable) -> Unit = {},
    private val onBatch: (GroupBatch) -> Unit
) : AutoCloseable {
    private val batches = ConcurrentHashMap<Any, Batch>()

    /**
     * Adds [message] to the batch for specified [sessionGroup].
     * Method is also updates the [Message.id] by setting `timestamp` to the current timestamp
     */
    fun onMessage(message: Message.Builder<*>, sessionGroup: String) {
        batches.getOrPut(batchSelector(message, sessionGroup)) { Batch(book, sessionGroup) }
            .add(message)
    }

    override fun close() {
        batches.values.forEach {
            it.close()
        }
    }

    inner class Batch(
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

        fun add(message: Message.Builder<*>) = lock.withLock {
            message.idBuilder().setTimestamp(Instant.now())
            batch.addGroup(message.build().toGroup())

            when (batch.groupsBuilder().size) {
                maxBatchSize -> send()
                1 -> future = executor.schedule(::send, maxFlushTime, MILLISECONDS)
            }
        }

        private fun send() = lock.withLock<Unit> {
            if (batch.groupsBuilder().isEmpty()) return
            runCatching { onBatch(batch.build()) }.onFailure(onError)
            batch = newBatch()
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