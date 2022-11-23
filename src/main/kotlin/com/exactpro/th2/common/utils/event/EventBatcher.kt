/*
 * Copyright 2022-2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.utils.event

import com.exactpro.th2.common.grpc.Event
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.utils.CheckedConsumer
import com.google.common.cache.CacheBuilder
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Future
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit.MILLISECONDS
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Collects and groups events by their parent-event-id and calls `onBatch` method when `maxFlushTime`
 * for a group has elapsed or number of events in it has reached `maxBatchSize`.
 */
class EventBatcher(
    private val maxBatchSize: Int = 100,
    private val maxFlushTime: Long = 1000,
    private val executor: ScheduledExecutorService,
    private val batchConsumer: CheckedConsumer<EventBatch>,
) : AutoCloseable {
    private val batches = CacheBuilder.newBuilder()
        .concurrencyLevel(Runtime.getRuntime().availableProcessors())
        .expireAfterAccess(maxFlushTime * 2, MILLISECONDS)
        .build<EventID, Batch>()
        .asMap()

    fun onEvent(event: Event) = batches.getOrPut(event.parentId, ::Batch).add(event)

    override fun close() = batches.values.forEach(Batch::close)

    private inner class Batch : AutoCloseable {
        private val lock = ReentrantLock()
        private var batch = EventBatch.newBuilder()
        private var future: Future<*> = CompletableFuture.completedFuture(null)

        fun add(event: Event) = lock.withLock {
            batch.addEvents(event)

            when (batch.eventsCount) {
                1 -> future = executor.schedule(::send, maxFlushTime, MILLISECONDS)
                maxBatchSize -> send()
            }
        }

        private fun send() = lock.withLock<Unit> {
            if (batch.eventsCount == 0) return
            batch.build().runCatching(batchConsumer::consume)
            batch.clearEvents()
            future.cancel(false)
        }

        override fun close() = send()
    }
}
