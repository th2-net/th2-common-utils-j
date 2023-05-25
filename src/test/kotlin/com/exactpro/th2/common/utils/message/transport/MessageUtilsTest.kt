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
package com.exactpro.th2.common.utils.message.transport

import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class MessageUtilsTest {

    @Test
    fun toTreeTable() {
        val transport = ParsedMessage.builder().apply {
            setType("test-type")
            bodyBuilder().apply {
                put("null", null)
                put("simple", "test-simple")
                put("int", 12345)
                put("float", 12345.12345)
                put("list", listOf("simple", null))
                put(
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
            }
        }.build()

        assertEquals(
            """
            {
              "type": "treeTable",
              "rows": {
                "null": { "type": "row", "columns": { "fieldValue": "null" } },
                "simple": { "type": "row", "columns": { "fieldValue": "test-simple" } },
                "float": { "type": "row", "columns": { "fieldValue": "12345.12345" } },
                "list": {
                  "type": "collection",
                  "rows": {
                    "0": { "type": "row", "columns": { "fieldValue": "simple" } },
                    "1": { "type": "row", "columns": { "fieldValue": "null" } }
                  }
                },
                "map": {
                  "type": "collection",
                  "rows": {
                    "null": { "type": "row", "columns": { "fieldValue": "null" } },
                    "simple": { "type": "row", "columns": { "fieldValue": "test-sub-simple" } },
                    "float": { "type": "row", "columns": { "fieldValue": "12345.12345" } },
                    "list": {
                      "type": "collection",
                      "rows": {
                        "0": { "type": "row", "columns": { "fieldValue": "simple" } },
                        "1": { "type": "row", "columns": { "fieldValue": "null" } }
                      }
                    },
                    "map": {
                      "type": "collection",
                      "rows": {
                        "null": { "type": "row", "columns": { "fieldValue": "null" } },
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
        """.trimIndent().replace("\n", "").replace(" ", ""), OBJECT_MAPPER.writeValueAsString(transport.toTreeTable())
        )
    }

    @Test
    fun `addMetadataProperty test`() {
        val message = ParsedMessage.builder()
            .setType("test")
            .setBody(mapOf("test" to null))
            .addMetadataProperty("A", "a")
            .addMetadataProperty("B", "b")
            .build()

        assertEquals("a", message.metadata["A"])
        assertEquals("b", message.metadata["B"])
    }

    @Test
    fun `addField test`() {
        val message = ParsedMessage.builder()
            .setType("test")
            .addField("A", "a")
            .addField("B", "b")
            .build()

        assertEquals("a", message.body["A"])
        assertEquals("b", message.body["B"])
    }


    companion object {
        val OBJECT_MAPPER = ObjectMapper()
    }
}