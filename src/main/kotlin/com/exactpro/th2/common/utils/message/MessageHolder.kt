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

import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.Value
import com.exactpro.th2.common.message.messageType
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.utils.event.transport.toProto
import com.exactpro.th2.common.utils.message.transport.toProto


sealed interface MessageHolder {
    val id: MessageID
    val parentEventId: EventID?

    // transport ids can be appeared here

    val messageType: String
    val protocol: String
    val properties: Map<String, String>

    val protoMessage: Message

    /**
     * Traverses the internal message and returns simple value by [path]
     * @return null when the last element exist but has null value otherwise return [String] value
     * @throws [FieldNotFoundException] if message doesn't include full path or message structure doesn't match to path or the target field isn't simple type
     */
    @Throws(FieldNotFoundException::class)
    fun getSimple(vararg path: String): String?
}

class ProtoMessageHolder(
    val source: Message
) : MessageHolder {
    override val id: MessageID
        get() = source.metadata.id
    override val parentEventId: EventID?
        get() = source.parentEventId
    override val messageType: String
        get() = source.messageType
    override val protocol: String
        get() = source.metadata.protocol
    override val properties: Map<String, String> = this.source.metadata.propertiesMap
    override val protoMessage: Message
        get() = source

    @Throws(FieldNotFoundException::class)
    override fun getSimple(vararg path: String): String? = source.getSimple(*path)

    override fun toString(): String {
        return source.toJson()
    }

    companion object {
        @JvmStatic
        val DEFAULT = ProtoMessageHolder(Message.getDefaultInstance())

        /**
         * Traverses the internal message and returns [Value] by [path]
         * @throws [FieldNotFoundException] if message doesn't include full path or message structure doesn't match to path
         */
        @Throws(FieldNotFoundException::class)
        fun Message.getField(vararg path: String): Value? = runCatching {
            require(path.isNotEmpty()) {
                "Path to field can't be empty"
            }
            var currentValue: Value? = fieldsMap[path.first()]

            path.asSequence().drop(1).forEachIndexed { pathIndex, name ->
                currentValue?.let {
                    currentValue = when (it.kindCase) {
                        Value.KindCase.MESSAGE_VALUE -> it.messageValue.fieldsMap[name]
                        Value.KindCase.LIST_VALUE -> {
                            val index = requireNotNull(name.toIntOrNull()) {
                                "'$name' path element can't be path as number, value: ${toJson()}, path: ${path.contentToString()}, index: ${pathIndex + 1}"
                            }
                            require(index >= 0 && it.listValue.valuesCount > index) {
                                "'$index' index should be positive or zero and less then '${it.listValue.valuesCount}' list size, value: ${toJson()}, path: ${path.contentToString()}, index: ${pathIndex + 1}"
                            }
                            it.listValue.getValues(index)
                        }

                        else -> error("Field '$name' can't be got from unknown value: ${toJson()}, path: ${path.contentToString()}, index: ${pathIndex + 1}")
                    }
                }
                    ?: error("Field '$name' is not found because '${path[pathIndex]}' previous field is null, path: ${path.contentToString()}, index: ${pathIndex + 1}")

            }
            currentValue
        }.getOrElse {
            throw FieldNotFoundException(
                "Filed not found by ${path.contentToString()} path in ${toJson()} message", it
            )
        }

        @Throws(FieldNotFoundException::class)
        fun Message.getSimple(vararg path: String): String? = getField(*path)?.run {
            when (kindCase) {
                Value.KindCase.NULL_VALUE -> null
                Value.KindCase.SIMPLE_VALUE -> simpleValue
                else -> throw FieldNotFoundException("Value by ${path.contentToString()} path isn't simple, value: ${this.toJson()}, message: ${toJson()}")
            }
        }
    }
}

class TransportMessageHolder(
    val source: ParsedMessage,
    val book: String,
    val sessionGroup: String
) : MessageHolder {
    override val id: MessageID by lazy { source.id.toProto(book, sessionGroup) }
    override val parentEventId: EventID? by lazy { source.eventId?.toProto() }
    override val messageType: String
        get() = source.type
    override val protocol: String
        get() = source.protocol
    override val properties: Map<String, String>
        get() = this.source.metadata
    override val protoMessage: Message by lazy { source.toProto(book, sessionGroup) }

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
                            "'$name' path element can't be path as number, value: ${currentValue}, path: ${path.contentToString()}, index: ${pathIndex + 1}"
                        }
                        val casted = (currentValue as List<*>)
                        require(index >= 0 && casted.size > index) {
                            "'$index' index should be positive or zero and less then '${casted.size}' list size, value: ${currentValue}, path: ${path.contentToString()}, index: ${pathIndex + 1}"
                        }
                        casted[index]
                    }

                    else -> error("Field '$name' can't be got from unknown value: ${currentValue}, path: ${path.contentToString()}, index: ${pathIndex + 1}")
                }
            }
            currentValue
        }.getOrElse {
            throw FieldNotFoundException("Filed not found by ${path.contentToString()} path in $this message", it)
        }

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