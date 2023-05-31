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

package com.exactpro.th2.common.utils

import mu.KotlinLogging
import java.util.concurrent.ExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.SECONDS

private val K_LOGGER = KotlinLogging.logger {}

fun ExecutorService.shutdownGracefully(timeout: Long = 3, unit: TimeUnit = SECONDS) {
    shutdown()
    if (!awaitTermination(timeout, unit)) {
        val neverCommencedTasks = shutdownNow()
        K_LOGGER.warn { "$this executor service can't be stopped gracefully, ${neverCommencedTasks.size} tasks will never be commenced" }
    }
}