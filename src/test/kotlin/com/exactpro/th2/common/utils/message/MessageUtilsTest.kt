/*
 * Copyright 2023-2026 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.message.addField
import com.exactpro.th2.common.message.get
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.EventId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.value.toValue
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Instant
import kotlin.random.Random

class MessageUtilsTest {
    @Test
    fun `proto to transport`() {
        val timestampValue = Instant.now()
        val sequenceValue = Random.nextLong()
        val subsequenceValue = (1..Random.nextInt(2, 5)).map { Random.nextInt() }
        val proto = Message.newBuilder().apply {
            metadataBuilder.apply {
                idBuilder.apply {
                    bookName = BOOK_NAME
                    connectionIdBuilder.apply {
                        sessionGroup = SESSION_GROUP
                        sessionAlias = SESSION_ALIAS
                    }
                    direction = com.exactpro.th2.common.grpc.Direction.SECOND
                    timestamp = timestampValue.toTimestamp()
                    sequence = sequenceValue
                    addAllSubsequence(subsequenceValue)
                }
                parentEventIdBuilder.apply {
                    bookName = BOOK_NAME
                    scope = SCOPE
                    startTimestamp = timestampValue.toTimestamp()
                    id = EVENT_ID
                }
                protocol = PROTOCOL
                messageType = MESSAGE_TYPE
                putProperties("test-property", "test-property-value")
            }
            addField("null", null)
            addField("simple", "test-simple")
            addField("list", listOf("simple", null))
            addField(
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
        }.build()
        val expectedTransport = ParsedMessage.builder().apply {
            idBuilder().apply {
                setSessionAlias(SESSION_ALIAS)
                setDirection(Direction.OUTGOING)
                setTimestamp(timestampValue)
                setSequence(sequenceValue)
                setSubsequence(subsequenceValue)
            }
            setEventId(EventId.builder().apply {
                setId(EVENT_ID)
                setBook(BOOK_NAME)
                setScope(SCOPE)
                setTimestamp(timestampValue)
            }.build())
            setProtocol(PROTOCOL)
            setType(MESSAGE_TYPE)
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
        }.build()

        assertEquals(expectedTransport, proto.toTransport())
    }

    @Test
    fun toTreeTable() {
        val proto = Message.newBuilder().apply {
            metadataBuilder.apply {
                messageType = MESSAGE_TYPE
            }
            addField("null", null)
            addField("simple", "test-simple")
            addField("int", 12345)
            addField("float", 12345.12345)
            addField("list", listOf("simple", null))
            addField(
                "map", mapOf(
                    "null" to null,
                    "simple" to "test-sub-simple",
                    "int" to 12345,
                    "float" to 12345.12345,
                    "list" to listOf("simple", null),
                    "map" to mapOf(
                        "null" to null,
                        "simple" to "test-sub-sub-simple",
                        "int" to 12345,
                        "float" to 12345.12345,
                    )
                )
            )
        }.build()

        assertEquals(
            """
            {
              "type": "treeTable",
              "rows": {
                "null": { "type": "row", "columns": { "fieldValue": null } },
                "simple": { "type": "row", "columns": { "fieldValue": "test-simple" } },
                "float": { "type": "row", "columns": { "fieldValue": "12345.12345" } },
                "list": {
                  "type": "collection",
                  "rows": {
                    "0": { "type": "row", "columns": { "fieldValue": "simple" } },
                    "1": { "type": "row", "columns": { "fieldValue": null } }
                  }
                },
                "map": {
                  "type": "collection",
                  "rows": {
                    "null": { "type": "row", "columns": { "fieldValue": null } },
                    "simple": { "type": "row", "columns": { "fieldValue": "test-sub-simple" } },
                    "float": { "type": "row", "columns": { "fieldValue": "12345.12345" } },
                    "list": {
                      "type": "collection",
                      "rows": {
                        "0": { "type": "row", "columns": { "fieldValue": "simple" } },
                        "1": { "type": "row", "columns": { "fieldValue": null } }
                      }
                    },
                    "map": {
                      "type": "collection",
                      "rows": {
                        "null": { "type": "row", "columns": { "fieldValue": null } },
                        "simple": { "type": "row", "columns": { "fieldValue": "test-sub-sub-simple" } },
                        "float": { "type": "row", "columns": { "fieldValue": "12345.12345" } },
                        "int": { "type": "row", "columns": { "fieldValue": "12345" } }
                      }
                    },
                    "int": { "type": "row", "columns": { "fieldValue": "12345" } }
                  }
                },
                "int": { "type": "row", "columns": { "fieldValue": "12345" } }
              }
            }
        """.trimIndent().replace("\n", "").replace(" ", ""), OBJECT_MAPPER.writeValueAsString(proto.toTreeTable())
        )
    }

    @Test
    fun `get list value`() {
        val message = mapOf(
            "null" to null,
            "list" to listOf("simple", null),
            "map" to mapOf(
                "null" to null,
                "list" to listOf("simple", null),
            ),
            "map-list" to listOf(
                mapOf(
                    "null" to null,
                    "list" to listOf("simple", null),
                ),
                null
            )
        ).toValue().messageValue

        Assertions.assertNull(message.getList("fake"))
        Assertions.assertNull(message.getList("null"))
        Assertions.assertNull(message.getList("list", "1"))
        Assertions.assertNull(message.getList("map", "fake"))
        Assertions.assertNull(message.getList("map", "null"))
        Assertions.assertNull(message.getList("map-list", "1"))
        Assertions.assertNull(message.getList("map-list", "0", "null"))
        Assertions.assertNull(message.getList("map-list", "0", "list", "1"))

        assertEquals(message["list"]?.listValue?.valuesList ?: error("Expected value problem"), message.getList("list"))
        assertEquals(
            message["map-list"]?.listValue?.valuesList ?: error("Expected value problem"),
            message.getList("map-list")
        )
        assertEquals(
            message["map"]?.messageValue?.getField("list")?.listValue?.valuesList ?: error("Expected value problem"),
            message.getList("map", "list")
        )
        assertEquals(
            message["map-list"]?.listValue?.getValues(0)?.messageValue?.getField("list")?.listValue?.valuesList
                ?: error("Expected value problem"), message.getList("map-list", "0", "list")
        )
    }

    @Test
    fun `get map value`() {
        val message = mapOf(
            "null" to null,
            "map" to mapOf(
                "null" to null,
            ),
            "map-list" to listOf(
                mapOf(
                    "null" to null,
                ),
                null
            )
        ).toValue().messageValue

        Assertions.assertNull(message.getMessage("fake"))
        Assertions.assertNull(message.getMessage("null"))
        Assertions.assertNull(message.getMessage("map", "fake"))
        Assertions.assertNull(message.getMessage("map", "null"))
        Assertions.assertNull(message.getMessage("map-list", "1"))
        Assertions.assertNull(message.getMessage("map-list", "0", "null"))

        assertEquals(
            message["map"]?.messageValue ?: error("Expected value problem"),
            message.getMessage("map")
        )
        assertEquals(
            message["map-list"]?.listValue?.getValues(0)?.messageValue ?: error("Expected value problem"),
            message.getMessage("map-list", "0")
        )
    }

    companion object {
        private const val BOOK_NAME = "test-book"
        private const val SESSION_GROUP = "test-session-group"
        private const val SESSION_ALIAS = "test-session-alias"
        private const val SCOPE = "test-scope"
        private const val EVENT_ID = "test-id"
        private const val PROTOCOL = "test-protocol"
        private const val MESSAGE_TYPE = "test-type"

        val OBJECT_MAPPER = ObjectMapper()
    }
}