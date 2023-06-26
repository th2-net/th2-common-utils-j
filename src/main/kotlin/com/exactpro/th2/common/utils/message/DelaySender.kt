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

import mu.KotlinLogging
import java.lang.Exception
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.math.ceil

class DelaySender<M>(
    size: Int,
    private val timeout: M.() -> Long?,
    private val onOverflow: () -> Unit,
    private val onEmpty: () -> Unit,
    private val onNegative: (M) -> Unit,
    private val onSend: (M) -> Unit,
) : AutoCloseable {

    private val queue = ArrayBlockingQueue<M>(size)
    private val executor = Thread(this::run, "delay-sender").apply {
        priority = Thread.MAX_PRIORITY
    }

    @Volatile
    private var strategy: () -> Boolean = this::process
    private val alive = AtomicBoolean(true)

    private val skip: Int = ceil(size * 0.1).toInt() //TODO: rename and make configurable
    private val dryList = mutableListOf<M>()

    init {
        executor.start() // this tread should be starter after instance initialization
    }

    fun put(message: M) {
        require(alive.get()) { "${this::class.simpleName} is closed" }
        if (!queue.offer(message)) {
            strategy = this::dry
            onOverflow()
            queue.put(message)
        }
    }

    override fun close() {
        if (alive.compareAndSet(true, false)) {
            executor.interrupt()
            executor.join(5_000)
            if (executor.isAlive) {
                K_LOGGER.warn { "${this::class.simpleName} can't be closed" }
            } else {
                K_LOGGER.info { "${this::class.simpleName} is closed successfully" }
            }
        }
    }

    private fun run() {
        K_LOGGER.info { "${this::class.simpleName} started" }
        try {
            while (strategy()) {
                // do nothing
            }
        } catch (e: InterruptedException) {
            K_LOGGER.info { "${this::class.simpleName} stopped" }
        } catch (e: Exception) {
            K_LOGGER.error(e) { "${this::class.simpleName} crushed" }
        }
    }

    private fun process(): Boolean {
        val message = queue.poll() ?: run {
            if (alive.get()) {
                onEmpty()
                queue.take()
            } else {
                return false
            }
        }
        sleep(message)
        onSend(message)
        return true
    }

    private fun dry(): Boolean {
        if(queue.drainTo(dryList) != 0) {
            dryList.forEachIndexed { index, message ->
                if (index % skip == 0) {
                    sleep(message)
                }
                onSend(message)
            }
            dryList.clear()
        }
        strategy = this::process
        return true
    }

    private fun sleep(message: M) {
        (message.timeout() ?: 0).apply {
            when {
                this < 0 -> onNegative(message)
                this > 0 -> {
                    val finish = System.nanoTime() + this
                    while (System.nanoTime() < finish) {
                        // do nothing
                    }
                }
            }
        }

    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger {}
    }
}