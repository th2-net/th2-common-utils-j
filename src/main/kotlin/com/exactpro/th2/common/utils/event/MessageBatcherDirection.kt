package com.exactpro.th2.common.utils.event

import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.Direction
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

class MessageBatcherDirection(
    private val maxBatchSize: Int = 100,
    private val maxFlushTime: Long = 1000,
    private val executor: ScheduledExecutorService,
    private val onBatch: (MessageGroupBatch, Direction) -> Unit,
    private val onError: (Throwable) -> Unit = {}
) : AutoCloseable {
    private val firstBatches = ConcurrentHashMap<String, MessageBatch>()
    private val secondBatches = ConcurrentHashMap<String, MessageBatch>()

    private fun putIntoDirection(messages: MessageGroup, alias: String, direction: Direction) {
        when(direction) {
            Direction.FIRST -> firstBatches.getOrPut(alias) { MessageBatch(direction) }.add(messages)
            Direction.SECOND -> secondBatches.getOrPut(alias) { MessageBatch(direction) }.add(messages)
            else -> error("Does not support direction: $direction")
        }
    }

    fun onMessage(message: RawMessage, direction: Direction) = putIntoDirection(message.toGroup(), message.sessionAliasOrEmpty, direction)
    fun onMessage(message: Message, direction: Direction) = putIntoDirection(message.toGroup(), message.sessionAliasOrEmpty, direction)
    fun onMessage(message: AnyMessage, direction: Direction) = putIntoDirection(message.toGroup(), message.sessionAliasOrEmpty, direction)
    fun onGroup(group: MessageGroup, direction: Direction) = putIntoDirection(group, group.sessionAliasOrEmpty, direction)

    override fun close() {
        firstBatches.values.forEach(MessageBatch::close)
        secondBatches.values.forEach(MessageBatch::close)
    }

    private inner class MessageBatch(val direction: Direction) : AutoCloseable {
        private val lock = ReentrantLock()
        private var batch = MessageGroupBatch.newBuilder()
        private var future: Future<*> = CompletableFuture.completedFuture(null)

        fun add(group: MessageGroup) = lock.withLock {
            batch.addGroups(group)

            when (batch.groupsCount) {
                1 -> future = executor.schedule(::send, maxFlushTime, MILLISECONDS)
                maxBatchSize -> send()
            }
        }

        private fun send() = lock.withLock<Unit> {
            if (batch.groupsCount == 0) return
            runCatching { onBatch(batch.build(), direction) }.onFailure(onError)
            batch.clearGroups()
            future.cancel(false)
        }

        override fun close() = send()
    }
}