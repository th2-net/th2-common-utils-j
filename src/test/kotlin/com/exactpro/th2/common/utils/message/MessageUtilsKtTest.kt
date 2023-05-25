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

import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.EventId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.utils.message.transport.toProto
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Instant
import kotlin.random.Random

class MessageUtilsKtTest {
    @Test
    fun `proto to transport and back test`() {
        val transport = ParsedMessage.builder().apply {
            idBuilder().apply {
                setSessionAlias("test-session-alias")
                setDirection(Direction.OUTGOING)
                setTimestamp(Instant.now())
                setSequence(Random.nextLong())
                setSubsequence((1..Random.nextInt(2, 5)).map { Random.nextInt() })
            }
            setEventId(EventId.builder().apply {
                setId("test-id")
                setBook(BOOK_NAME)
                setScope("test-scope")
                setTimestamp(Instant.now())
            }.build())
            setProtocol("test-protocol")
            setType("test-type")
            metadataBuilder().apply {
                put("test-property", "test-property-value")
            }
            bodyBuilder().apply {
                put("null", null)
                put("simple", "test-simple")
                put("list", listOf("simple", null))
                put(
                    "map", mapOf(
                        "null" to null,
                        "simple" to "test-sub-simple",
                        "list" to listOf("simple", null),
                        "map" to mapOf(
                            "null" to null,
                            "simple" to "test-sub-sub-simple",
                        )
                    )
                )
            }
            setType("test-type")
        }.build()

        val proto = transport.toProto(BOOK_NAME, SESSION_GROUP)
        assertEquals(BOOK_NAME, proto.metadata.id.bookName)
        assertEquals(SESSION_GROUP, proto.metadata.id.connectionId.sessionGroup)
        proto.toTransport().apply {
            assertEquals(transport, this)
        }
    }

    companion object {
        const val BOOK_NAME = "test-book"
        val SESSION_GROUP = "test-session-group"
    }
}