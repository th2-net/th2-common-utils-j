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

package com.exactpro.th2.common.utils.message.proto

import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.message.addField
import com.exactpro.th2.common.value.nullValue
import com.exactpro.th2.common.value.toValue
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class ProtoMessageWrapperTest {
    @Test
    fun `get simple test`() {
        val wrapper = ProtoMessageWrapper(Message.newBuilder().apply {
            metadataBuilder.apply {
                messageType = "test-type"
            }
            addField(NULL_FIELD, nullValue())
            addField(SIMPLE_FIELD, TEST_SIMPLE.toValue())
            addField("list-simple", listOf(TEST_SIMPLE, null).toValue())
            addField(
                "list-messages", listOf(
                    mapOf(
                        NULL_FIELD to null,
                        SIMPLE_FIELD to TEST_SIMPLE,
                    ), null
                ).toValue()
            )
            addField(
                "map", mapOf(
                    NULL_FIELD to null,
                    SIMPLE_FIELD to TEST_SIMPLE,
                    "list-simple" to listOf(TEST_SIMPLE, null),
                    "sub-map" to mapOf(
                        NULL_FIELD to null,
                        SIMPLE_FIELD to TEST_SIMPLE,
                    )
                ).toValue()
            )
        }.build())

        Assertions.assertNull(wrapper.getSimple(FAKE_FIELD))
        Assertions.assertNull(wrapper.getSimple(NULL_FIELD))
        Assertions.assertNull(wrapper.getSimple("list-simple", "1"))
        Assertions.assertNull(wrapper.getSimple("list-messages", "0", NULL_FIELD))
        Assertions.assertNull(wrapper.getSimple("list-messages", "0", FAKE_FIELD))
        Assertions.assertNull(wrapper.getSimple("list-messages", "1"))
        Assertions.assertNull(wrapper.getSimple("map", NULL_FIELD))
        Assertions.assertNull(wrapper.getSimple("map", FAKE_FIELD))
        Assertions.assertNull(wrapper.getSimple("map", "list-simple", "1"))
        Assertions.assertNull(wrapper.getSimple("map", "sub-map", NULL_FIELD))
        Assertions.assertNull(wrapper.getSimple("map", "sub-map", FAKE_FIELD))

        Assertions.assertEquals(TEST_SIMPLE, wrapper.getSimple(SIMPLE_FIELD))
        Assertions.assertEquals(TEST_SIMPLE, wrapper.getSimple("list-simple", "0"))
        Assertions.assertEquals(TEST_SIMPLE, wrapper.getSimple("list-messages", "0", SIMPLE_FIELD))
        Assertions.assertEquals(TEST_SIMPLE, wrapper.getSimple("map", SIMPLE_FIELD))
        Assertions.assertEquals(TEST_SIMPLE, wrapper.getSimple("map", "list-simple", "0"))
        Assertions.assertEquals(TEST_SIMPLE, wrapper.getSimple("map", "sub-map", SIMPLE_FIELD))
    }

    companion object {
        private const val TEST_SIMPLE = "test-simple"

        private const val NULL_FIELD = "null"
        private const val FAKE_FIELD = "Fake"
        private const val SIMPLE_FIELD = "simple"
    }
}