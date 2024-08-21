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

import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.message.addField
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.utils.message.transport.convertToString
import com.exactpro.th2.common.value.nullValue
import com.exactpro.th2.common.value.toValue
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class MessageHolderTest {
    @Test
    fun `get simple proto test`() {
        val wrapper = ProtoMessageHolder(Message.newBuilder().apply {
            metadataBuilder.apply {
                messageType = MESSAGE_TYPE
            }
            addField(NULL_FIELD, nullValue())
            addField(SIMPLE_FIELD, TEST_SIMPLE.toValue())
            addField(LIST_SIMPLE_FIELD, listOf(TEST_SIMPLE, null).toValue())
            addField(
                LIST_MESSAGES_FIELD, listOf(
                    mapOf(
                        NULL_FIELD to null,
                        SIMPLE_FIELD to TEST_SIMPLE,
                    ), null
                ).toValue()
            )
            addField(
                MAP_FIELD, mapOf(
                    NULL_FIELD to null,
                    SIMPLE_FIELD to TEST_SIMPLE,
                    LIST_SIMPLE_FIELD to listOf(TEST_SIMPLE, null),
                    "sub-map" to mapOf(
                        NULL_FIELD to null,
                        SIMPLE_FIELD to TEST_SIMPLE,
                    )
                ).toValue()
            )
        }.build())

        Assertions.assertNull(wrapper.getSimple(FAKE_FIELD))
        Assertions.assertNull(wrapper.getSimple(NULL_FIELD))
        Assertions.assertNull(wrapper.getSimple(LIST_SIMPLE_FIELD, "1"))
        Assertions.assertNull(wrapper.getSimple(LIST_MESSAGES_FIELD, "0", NULL_FIELD))
        Assertions.assertNull(wrapper.getSimple(LIST_MESSAGES_FIELD, "0", FAKE_FIELD))
        Assertions.assertNull(wrapper.getSimple(LIST_MESSAGES_FIELD, "1"))
        Assertions.assertNull(wrapper.getSimple(MAP_FIELD, NULL_FIELD))
        Assertions.assertNull(wrapper.getSimple(MAP_FIELD, FAKE_FIELD))
        Assertions.assertNull(wrapper.getSimple(MAP_FIELD, LIST_SIMPLE_FIELD, "1"))
        Assertions.assertNull(wrapper.getSimple(MAP_FIELD, "sub-map", NULL_FIELD))
        Assertions.assertNull(wrapper.getSimple(MAP_FIELD, "sub-map", FAKE_FIELD))

        Assertions.assertEquals(TEST_SIMPLE, wrapper.getSimple(SIMPLE_FIELD))
        Assertions.assertEquals(TEST_SIMPLE, wrapper.getSimple(LIST_SIMPLE_FIELD, "0"))
        Assertions.assertEquals(TEST_SIMPLE, wrapper.getSimple(LIST_MESSAGES_FIELD, "0", SIMPLE_FIELD))
        Assertions.assertEquals(TEST_SIMPLE, wrapper.getSimple(MAP_FIELD, SIMPLE_FIELD))
        Assertions.assertEquals(TEST_SIMPLE, wrapper.getSimple(MAP_FIELD, LIST_SIMPLE_FIELD, "0"))
        Assertions.assertEquals(TEST_SIMPLE, wrapper.getSimple(MAP_FIELD, "sub-map", SIMPLE_FIELD))
    }

    @Test
    fun `get simple transport test`() {
        val wrapper = TransportMessageHolder(ParsedMessage.builder().apply {
            setType(MESSAGE_TYPE)
            bodyBuilder().apply {
                put(NULL_FIELD, null)
                put(SIMPLE_FIELD, TEST_SIMPLE)
                put(INT_FIELD, TEST_INT)
                put(FLOAT_FIELD, TEST_FLOAT)
                put(LIST_SIMPLE_FIELD, listOf(TEST_SIMPLE, null))
                put(
                    LIST_MESSAGES_FIELD, listOf(
                        mapOf(
                            NULL_FIELD to null,
                            SIMPLE_FIELD to TEST_SIMPLE,
                            INT_FIELD to TEST_INT,
                            FLOAT_FIELD to TEST_FLOAT,
                        ),
                        null
                    )
                )
                put(
                    MAP_FIELD, mapOf(
                        NULL_FIELD to null,
                        SIMPLE_FIELD to TEST_SIMPLE,
                        INT_FIELD to TEST_INT,
                        FLOAT_FIELD to TEST_FLOAT,
                        LIST_SIMPLE_FIELD to listOf(TEST_SIMPLE, null),
                        "sub-map" to mapOf(
                            NULL_FIELD to null,
                            SIMPLE_FIELD to TEST_SIMPLE,
                            INT_FIELD to TEST_INT,
                            FLOAT_FIELD to TEST_FLOAT,
                        )
                    )
                )
            }
        }.build(), "test-book", "test-session-group")

        Assertions.assertNull(wrapper.getSimple(FAKE_FIELD))
        Assertions.assertNull(wrapper.getSimple(NULL_FIELD))
        Assertions.assertNull(wrapper.getSimple(LIST_SIMPLE_FIELD, "1"))
        Assertions.assertNull(wrapper.getSimple(LIST_MESSAGES_FIELD, "0", NULL_FIELD))
        Assertions.assertNull(wrapper.getSimple(LIST_MESSAGES_FIELD, "0", FAKE_FIELD))
        Assertions.assertNull(wrapper.getSimple(LIST_MESSAGES_FIELD, "1"))
        Assertions.assertNull(wrapper.getSimple(MAP_FIELD, NULL_FIELD))
        Assertions.assertNull(wrapper.getSimple(MAP_FIELD, FAKE_FIELD))
        Assertions.assertNull(wrapper.getSimple(MAP_FIELD, LIST_SIMPLE_FIELD, "1"))
        Assertions.assertNull(wrapper.getSimple(MAP_FIELD, "sub-map", NULL_FIELD))
        Assertions.assertNull(wrapper.getSimple(MAP_FIELD, "sub-map", FAKE_FIELD))

        Assertions.assertEquals(TEST_SIMPLE, wrapper.getSimple(SIMPLE_FIELD))
        Assertions.assertEquals(TEST_SIMPLE, wrapper.getSimple(LIST_SIMPLE_FIELD, "0"))
        Assertions.assertEquals(TEST_SIMPLE, wrapper.getSimple(LIST_MESSAGES_FIELD, "0", SIMPLE_FIELD))
        Assertions.assertEquals(TEST_SIMPLE, wrapper.getSimple(MAP_FIELD, SIMPLE_FIELD))
        Assertions.assertEquals(TEST_SIMPLE, wrapper.getSimple(MAP_FIELD, LIST_SIMPLE_FIELD, "0"))
        Assertions.assertEquals(TEST_SIMPLE, wrapper.getSimple(MAP_FIELD, "sub-map", SIMPLE_FIELD))

        Assertions.assertEquals("12345", TEST_INT.convertToString())
        Assertions.assertEquals(TEST_INT.toString(), wrapper.getSimple(INT_FIELD))
        Assertions.assertEquals(TEST_INT.toString(), wrapper.getSimple(LIST_MESSAGES_FIELD, "0", INT_FIELD))
        Assertions.assertEquals(TEST_INT.toString(), wrapper.getSimple(MAP_FIELD, INT_FIELD))
        Assertions.assertEquals(TEST_INT.toString(), wrapper.getSimple(MAP_FIELD, "sub-map", INT_FIELD))

        Assertions.assertEquals("1234512345678.9", TEST_FLOAT.convertToString())
        Assertions.assertEquals(TEST_FLOAT.toString(), wrapper.getSimple(FLOAT_FIELD))
        Assertions.assertEquals(TEST_FLOAT.toString(), wrapper.getSimple(LIST_MESSAGES_FIELD, "0", FLOAT_FIELD))
        Assertions.assertEquals(TEST_FLOAT.toString(), wrapper.getSimple(MAP_FIELD, FLOAT_FIELD))
        Assertions.assertEquals(TEST_FLOAT.toString(), wrapper.getSimple(MAP_FIELD, "sub-map", FLOAT_FIELD))
    }

    companion object {
        private const val MESSAGE_TYPE = "test-type"

        private const val TEST_SIMPLE = "test-simple"
        private const val TEST_INT = 12345
        private const val TEST_FLOAT = 1234512345678.9

        private const val NULL_FIELD = "null"
        private const val FAKE_FIELD = "Fake"
        private const val SIMPLE_FIELD = "simple"
        private const val INT_FIELD = "int"
        private const val FLOAT_FIELD = "float"
        private const val LIST_SIMPLE_FIELD = "list-simple"
        private const val LIST_MESSAGES_FIELD = "list-messages"
        private const val MAP_FIELD = "map"
    }
}