/*
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
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
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Future
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit.MILLISECONDS
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * The batcher collects single events inside and calls `onBatch` method when `maxFlushTime` has elapsed or number of pending events has reached `maxBatchSize`.
 */
class SingleEventBatcher(
    private val maxBatchSize: Int = 100,
    private val maxFlushTime: Long = 1000,
    private val executor: ScheduledExecutorService,
    private val onBatch: (EventBatch) -> Unit
) : AutoCloseable {

    private val lock = ReentrantLock()
    private var batch = EventBatch.newBuilder()
    private var future: Future<*> = CompletableFuture.completedFuture(null)

    fun onEvent(event: Event) = lock.withLock {
        batch.addEvents(event)

        when (batch.eventsCount) {
            1 -> future = executor.schedule(::send, maxFlushTime, MILLISECONDS)
            maxBatchSize -> send()
        }
    }

    override fun close() = send()

    private fun send() = lock.withLock<Unit> {
        if (batch.eventsCount == 0) return
        batch.build().runCatching(onBatch)
        batch.clearEvents()
        future.cancel(false)
    }
}