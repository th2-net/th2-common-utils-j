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

import com.exactpro.th2.common.event.bean.TreeTable
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.Value
import com.exactpro.th2.common.message.messageType
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.utils.message.FieldNotFoundException
import com.exactpro.th2.common.utils.message.MessageWrapper
import com.exactpro.th2.common.utils.message.toTreeTable
import com.google.protobuf.TextFormat

class ProtoMessageWrapper(
    val source: Message
) : MessageWrapper {
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
    override val treeTable: TreeTable by lazy { source.toTreeTable() }

    @Throws(FieldNotFoundException::class)
    override fun getSimple(vararg path: String): String? = source.getSimple(*path)

    override fun toString(): String {
        return TextFormat.shortDebugString(source)
    }

    companion object {
        @JvmStatic
        val DEFAULT = ProtoMessageWrapper(Message.getDefaultInstance())

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
        }.onFailure {
            throw FieldNotFoundException(
                "Filed not found by ${path.contentToString()} path in ${toJson()} message", it
            )
        }.getOrThrow()

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