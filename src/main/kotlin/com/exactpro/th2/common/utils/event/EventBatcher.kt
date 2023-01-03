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
    private val maxBatchSizeInBytes: Long = 1_024 * 1_024,
    private val maxBatchSizeInItems: Int = 100,
    private val maxFlushTime: Long = 1000,
    private val executor: ScheduledExecutorService,
    private val onBatch: (EventBatch) -> Unit,
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
        private var batchSizeInBytes = BATCH_LEN_CONST
        private var future: Future<*> = CompletableFuture.completedFuture(null)

        fun add(event: Event) = lock.withLock {
            val eventSizeInBytes = event.calculateSizeInBytes()
            if (batchSizeInBytes + eventSizeInBytes > maxBatchSizeInBytes) {
                send()
            }

            batch.addEvents(event)
            batchSizeInBytes += eventSizeInBytes

            when (batch.eventsCount) {
                1 -> future = executor.schedule(::send, maxFlushTime, MILLISECONDS)
                maxBatchSizeInItems -> send()
            }
        }

        override fun close() = send()

        private fun send() = lock.withLock<Unit> {
            if (batch.eventsCount == 0) return
            batch.build().runCatching(onBatch)
            batch.clearEvents()
            batchSizeInBytes = BATCH_LEN_CONST
            future.cancel(false)
        }
    }

    companion object {
        /**
		* 4 - magic number
		* 1 - protocol version
		* 4 - message sizes
		* Collapsed constant = 9
	    */
        private const val BATCH_LEN_CONST = 9L

        /**
        * 2 - magic number
        * 4 - id length
        * 4 - name length
        * 4 - type length
        * 4 - parent id length
        * 4 + 8 = Instant (start timestamp) long (seconds) + int (nanos)
        * 4 + 8 = Instant (end timestamp) long (seconds) + int (nanos)
        * 1 = is success
        * 4 = body len
        * ===
        * 47
        */
        private const val EVENT_RECORD_CONST = 47L
        private const val BATCH_LENGTH_IN_BATCH = 4L
        /**
         * This logic is copied the estore project.
         */
        internal fun Event.calculateSizeInBytes(): Long = EVENT_RECORD_CONST +
                BATCH_LENGTH_IN_BATCH +
                id.id.length +
                parentId.id.length +
                name.length +
                type.length +
                body.size()
    }
}
