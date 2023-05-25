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

import com.exactpro.th2.common.event.bean.TreeTable
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.utils.event.transport.toProto
import com.exactpro.th2.common.utils.message.FieldNotFoundException
import com.exactpro.th2.common.utils.message.MessageWrapper

class TransportMessageWrapper(
    val source: ParsedMessage,
    val book: String,
    val sessionGroup: String
) : MessageWrapper {
    override val id: MessageID by lazy { source.id.toProto(book, sessionGroup) }
    override val parentEventId: EventID? by lazy { source.eventId?.toProto() }
    override val messageType: String
        get() = source.type
    override val protocol: String
        get() = source.protocol
    override val properties: Map<String, String>
        get() = this.source.metadata
    override val protoMessage: Message by lazy { source.toProto(book, sessionGroup) }
    override val treeTable: TreeTable by lazy { source.toTreeTable() }

    @Throws(FieldNotFoundException::class)
    override fun getSimple(vararg path: String): String? = source.getSimple(*path)

    override fun toString(): String {
        return "TransportMessageWrapper(transport=$source, book='$book', sessionGroup='$sessionGroup')"
    }

    companion object {
        /**
         * Traverses the internal message and returns value by [path]
         * @return null when the last element exist but has null value otherwise return [Any] value
         * @throws [FieldNotFoundException] if message doesn't include full path or message structure doesn't match to path
         */
        @Throws(FieldNotFoundException::class)
        fun ParsedMessage.getField(vararg path: String): Any? = runCatching {
            require(path.isNotEmpty()) {
                "Path to field can't be empty"
            }
            var currentValue: Any? = body[path.first()]

            path.asSequence().drop(1).forEachIndexed { pathIndex, name ->
                currentValue = when (currentValue) {
                    is Map<*, *> -> (currentValue as Map<*, *>)[name]
                    is List<*> -> {
                        val index = requireNotNull(name.toIntOrNull()) {
                            "'$name' path element can't be path as number, value: ${currentValue}, path: ${path.contentToString()}, index: $pathIndex"
                        }
                        val casted = (currentValue as List<*>)
                        require(index >= 0 && casted.size > index) {
                            "'$index' index should be positive or zero and less then ${casted.size}, value: ${currentValue}, path: ${path.contentToString()}, index: $pathIndex"
                        }
                        casted[index]
                    }

                    else -> error("Field '$name' can't be got from unknown value: ${currentValue}, path: ${path.contentToString()}, index: $pathIndex")
                }
            }
            currentValue
        }.onFailure {
            throw FieldNotFoundException("Filed not found by ${path.contentToString()} path in $this message", it)
        }.getOrThrow()

        @Throws(FieldNotFoundException::class)
        fun ParsedMessage.getSimple(vararg path: String): String? = getField(*path)?.run {
            when (this) {
                is String -> this
                is Number -> this.toString()
                else -> throw FieldNotFoundException(
                    "Value by ${path.contentToString()} path isn't string, actual value: $this ${this::class.java.simpleName}, message: $this"
                )
            }
        }
    }
}