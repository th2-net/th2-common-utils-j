package com.exactpro.th2.common.utils.event

import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.RawMessage
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Future
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit.MILLISECONDS
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class MessageBatcher(
    private val maxBatchSize: Int = 100,
    private val maxFlushTime: Long = 1000,
    private val executor: ScheduledExecutorService,
    private val onBatch: (MessageGroupBatch) -> Unit
) : AutoCloseable {
    private val batches = ConcurrentHashMap<String, MessageBatch>()

    fun onMessage(message: RawMessage) = batches.getOrPut(message.metadata.id.connectionId.sessionAlias, ::MessageBatch).add(message)
    fun onMessage(message: Message) = batches.getOrPut(message.metadata.id.connectionId.sessionAlias, ::MessageBatch).add(message)
    fun onGroup(group: MessageGroup) = batches.getOrPut(group.getSessionAlias(), ::MessageBatch).add(group)

    override fun close() = batches.values.forEach(MessageBatch::close)

    private inner class MessageBatch : AutoCloseable {
        private val lock = ReentrantLock()
        private var batch = MessageGroupBatch.newBuilder()
        private var future: Future<*> = CompletableFuture.completedFuture(null)

        fun add(message: RawMessage) = lock.withLock {
            batch.addGroups(message.toGroup())

            when (batch.groupsCount) {
                1 -> future = executor.schedule(::send, maxFlushTime, MILLISECONDS)
                maxBatchSize -> send()
            }
        }

        fun add(group: MessageGroup) = lock.withLock {
            batch.addGroups(group)

            when (batch.groupsCount) {
                1 -> future = executor.schedule(::send, maxFlushTime, MILLISECONDS)
                maxBatchSize -> send()
            }
        }

        fun add(message: Message) = lock.withLock {
            batch.addGroups(message.toGroup())

            when (batch.groupsCount) {
                1 -> future = executor.schedule(::send, maxFlushTime, MILLISECONDS)
                maxBatchSize -> send()
            }
        }

        private fun send() = lock.withLock<Unit> {
            if (batch.groupsCount == 0) return
            batch.build().runCatching(onBatch)
            batch.clearGroups()
            future.cancel(false)
        }

        override fun close() = send()
    }
}