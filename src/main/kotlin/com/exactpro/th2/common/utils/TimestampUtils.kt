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

package com.exactpro.th2.common.utils

import com.google.protobuf.TimestampOrBuilder
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.time.ZoneOffset


@Suppress("SpellCheckingInspection")
private val LOG_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSSSSSSSS")
    .withZone(ZoneOffset.UTC)

fun TimestampOrBuilder.toInstant(): Instant = Instant.ofEpochSecond(seconds, nanos.toLong())

val TimestampOrBuilder.logTimestamp: String
    get() = LOG_DATE_TIME_FORMATTER.format(toInstant())