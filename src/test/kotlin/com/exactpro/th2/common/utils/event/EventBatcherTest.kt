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
import com.exactpro.th2.common.utils.event.EventBatcher.Companion.calculateSizeInBytes
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

class EventBatcherTest {

    @Test
    fun `max size in items test`() {
        val future = mock<ScheduledFuture<*>> { }
        val batchCaptor = argumentCaptor<EventBatch> { }
        val onBatch = mock<(EventBatch) -> Unit> {
            on { invoke(batchCaptor.capture()) }.thenReturn(null)
        }
        val executor = mock<ScheduledExecutorService> {
            on { schedule(any(), any(), any()) }.thenReturn(future)
        }
        EventBatcher(1_024 * 1_024, 2, Long.MAX_VALUE / 2, executor, onBatch).use { batcher ->
            batcher.onEvent(EVENT_1)
            verify(onBatch, times(0))(any())
            verify(executor, times(1)).schedule(any(), any(), any())
            verify(future, times(0)).cancel(any())

            batcher.onEvent(EVENT_2)
            verify(onBatch, times(1))(any())
            verify(executor, times(1)).schedule(any(), any(), any())
            verify(future, times(1)).cancel(any())

            assertAll(
                { assertEquals(2, batchCaptor.firstValue.eventsCount) },
                { assertEquals(EVENT_1, batchCaptor.firstValue.getEvents(0)) },
                { assertEquals(EVENT_2, batchCaptor.firstValue.getEvents(1)) },
            )
        }
    }

    @Test
    fun `max size in bytes test`() {
        val future = mock<ScheduledFuture<*>> { }
        val batchCaptor = argumentCaptor<EventBatch> { }
        val onBatch = mock<(EventBatch) -> Unit> {
            on { invoke(batchCaptor.capture()) }.thenReturn(null)
        }
        val executor = mock<ScheduledExecutorService> {
            on { schedule(any(), any(), any()) }.thenReturn(future)
        }
        EventBatcher(EVENT_SIZE_IN_BYTES * 2, Int.MAX_VALUE, Long.MAX_VALUE / 2, executor, onBatch).use { batcher ->
            batcher.onEvent(EVENT_1)
            verify(onBatch, times(0))(any())
            verify(executor, times(1)).schedule(any(), any(), any())
            verify(future, times(0)).cancel(any())

            batcher.onEvent(EVENT_2)
            verify(onBatch, times(1))(any())
            verify(executor, times(2)).schedule(any(), any(), any())
            verify(future, times(1)).cancel(any())

            batcher.onEvent(EVENT_3)
            verify(onBatch, times(2))(any())
            verify(executor, times(3)).schedule(any(), any(), any())
            verify(future, times(2)).cancel(any())

            assertAll(
                { assertEquals(1, batchCaptor.firstValue.eventsCount) },
                { assertEquals(EVENT_1, batchCaptor.firstValue.getEvents(0)) },
            )
            assertAll(
                { assertEquals(1, batchCaptor.secondValue.eventsCount) },
                { assertEquals(EVENT_2, batchCaptor.secondValue.getEvents(0)) },
            )
        }
    }

    @Test
    fun `max flush time`() {
        val maxFlushTime = 10L

        val future = mock<ScheduledFuture<*>> { }
        val runnableCaptor = argumentCaptor<Runnable> { }
        val batchCaptor = argumentCaptor<EventBatch> { }
        val onBatch = mock<(EventBatch) -> Unit> {
            on { invoke(batchCaptor.capture()) }.thenReturn(null)
        }
        val executor = mock<ScheduledExecutorService> {
            on { schedule(runnableCaptor.capture(), eq(maxFlushTime), eq(TimeUnit.MILLISECONDS)) }.thenReturn(future)
        }

        EventBatcher(1_024 * 1_024, Int.MAX_VALUE, maxFlushTime, executor, onBatch).use { batcher ->
            batcher.onEvent(EVENT_1)
            verify(onBatch, times(0))(any())
            verify(future, times(0)).cancel(any())
            runnableCaptor.firstValue.run()
            verify(onBatch, times(1))(any())
            verify(future, times(1)).cancel(any())
            assertAll(
                { assertEquals(1, batchCaptor.firstValue.eventsCount) },
                { assertEquals(EVENT_1, batchCaptor.firstValue.getEvents(0)) },
            )
        }
    }

    companion object {
        private val EVENT_1 = Event.newBuilder().apply {
            idBuilder.apply {
                id = "test_1"
            }
        }.build()
        private val EVENT_2 = Event.newBuilder().apply {
            idBuilder.apply {
                id = "test_2"
            }
        }.build()
        private val EVENT_3 = Event.newBuilder().apply {
            idBuilder.apply {
                id = "test_3"
            }
        }.build()

        private val EVENT_SIZE_IN_BYTES = maxOf(
            EVENT_1.calculateSizeInBytes(),
            EVENT_2.calculateSizeInBytes(),
            EVENT_3.calculateSizeInBytes()
        )
    }
}