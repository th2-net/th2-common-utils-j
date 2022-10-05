package com.exactpro.th2.common.utils.message

import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.RawMessage
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

class RawMessageBatcher(
    maxBatchSize: Int = 1000,
    maxFlushTime: Long = 1000,
    private val batchSelector: (RawMessage.Builder) -> Any,
    executor: ScheduledExecutorService,
    onError: (Throwable) -> Unit = {},
    onBatch: (MessageGroupBatch) -> Unit
): Batcher<RawMessage.Builder>(maxBatchSize,maxFlushTime, executor, onError, onBatch) {
    override fun onMessage(message: RawMessage.Builder) {
        message.metadataBuilder.timestamp = Instant.now().toTimestamp()
        add(batchSelector(message), message.build())
    }
}

class MessageBatcher(
    maxBatchSize: Int = 1000,
    maxFlushTime: Long = 1000,
    private val batchSelector: (Message.Builder) -> Any,
    executor: ScheduledExecutorService,
    onError: (Throwable) -> Unit = {},
    onBatch: (MessageGroupBatch) -> Unit
): Batcher<Message.Builder>(maxBatchSize,maxFlushTime, executor, onError, onBatch) {
    override fun onMessage(message: Message.Builder) {
        message.metadataBuilder.timestamp = Instant.now().toTimestamp()
        add(batchSelector(message), message.build())
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

    protected fun add(key: Any, message: RawMessage) = batches.getOrPut(key, ::Batch).add(message.toGroup())
    protected fun add(key: Any, message: Message) = batches.getOrPut(key, ::Batch).add(message.toGroup())
    protected fun add(key: Any, message: AnyMessage) = batches.getOrPut(key, ::Batch).add(message.toGroup())
    protected fun add(key: Any, group: MessageGroup) = batches.getOrPut(key, ::Batch).add(group)

    override fun close() {
        batches.values.forEach {
            it.close()
        }
    }

    protected inner class Batch : AutoCloseable {
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
            runCatching { onBatch(batch.build()) }.onFailure(onError)
            batch.clearGroups()
            future.cancel(false)
        }

        override fun close() = send()
    }
}
