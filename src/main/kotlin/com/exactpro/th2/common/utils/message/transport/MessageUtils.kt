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
import com.exactpro.th2.common.event.bean.TreeTableEntry
import com.exactpro.th2.common.event.bean.builder.CollectionBuilder
import com.exactpro.th2.common.event.bean.builder.RowBuilder
import com.exactpro.th2.common.event.bean.builder.TreeTableBuilder
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.message.addField
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.EventId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Message
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.proto
import com.exactpro.th2.common.utils.event.transport.toProto
import com.exactpro.th2.common.utils.message.FieldNotFoundException
import com.exactpro.th2.common.utils.message.MessageTableColumn
import com.exactpro.th2.common.value.toValue
import java.math.BigDecimal
import com.exactpro.th2.common.grpc.Message as ProtoMessage

fun MessageGroup.toBatch(book: String, sessionGroup: String): GroupBatch = GroupBatch.builder().apply {
    setBook(book)
    setSessionGroup(sessionGroup)
    addGroup(this@toBatch)
}.build()

val MessageGroup.eventIds: Sequence<EventId>
    get() = messages.asSequence()
        .map(Message<*>::eventId)
        .filterNotNull()
        .distinct()

fun Message<*>.toGroup(): MessageGroup = MessageGroup.builder().apply {
    addMessage(this@toGroup)
}.build()

fun MessageId.toProto(book: String, sessionGroup: String): MessageID = MessageID.newBuilder().also {
    it.bookName = book
    it.direction = direction.proto
    it.sequence = sequence
    it.timestamp = timestamp.toTimestamp()

    it.addAllSubsequence(subsequence)

    it.connectionIdBuilder.also { connectionId ->
        connectionId.sessionGroup = sessionGroup.ifBlank { sessionAlias }
        connectionId.sessionAlias = sessionAlias
    }
}.build()

fun MessageId.toProto(groupBatch: GroupBatch): MessageID = toProto(groupBatch.book, groupBatch.sessionGroup)

fun ParsedMessage.toProto(book: String, sessionGroup: String): ProtoMessage = ProtoMessage.newBuilder().apply {
    metadataBuilder.apply {
        id = this@toProto.id.toProto(book, sessionGroup)
        messageType = this@toProto.type
        protocol = this@toProto.protocol
        putAllProperties(this@toProto.metadata)
    }
    body.forEach { (key, value) -> addField(key, value.toValue()) }
    eventId?.let { parentEventId = it.toProto() }
}.build()

fun ParsedMessage.toTreeTable(): TreeTable = TreeTableBuilder().apply {
    for ((key, value) in body) {
        row(key, value.toTreeTableEntry())
    }
}.build()

private fun Any?.toTreeTableEntry(): TreeTableEntry {
    return when (this) {
        null -> RowBuilder()
            .column(MessageTableColumn(null))
            .build()

        is Map<*, *> -> {
            CollectionBuilder().apply {
                forEach { (key, value) ->
                    row(key.toString(), value.toTreeTableEntry())
                }
            }.build()
        }

        is List<*> -> {
            CollectionBuilder().apply {
                forEachIndexed { index, nestedValue ->
                    row(index.toString(), nestedValue.toTreeTableEntry())
                }
            }.build()
        }

        is String -> RowBuilder().column(MessageTableColumn(this)).build()
        is Number -> RowBuilder().column(MessageTableColumn(convertToString())).build()
        else -> error("Unsupported ${this::class.simpleName} number type, value $this")
    }
}

/**
 * Traverses the internal message and returns value by [path]
 * @return null when the last element exist but has null value otherwise return [Any] value
 * @throws [FieldNotFoundException] if message doesn't include full path or message structure doesn't match to path
 */
@Throws(FieldNotFoundException::class)
fun Map<*, *>.getField(vararg path: String): Any? = runCatching {
    require(path.isNotEmpty()) {
        "Path to field can't be empty"
    }
    var currentValue: Any? = this[path.first()]

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
fun Map<*, *>.getString(vararg path: String): String? = getField(*path)?.run {
    when (this) {
        is String -> this
        is Number -> this.toString()
        else -> throw FieldNotFoundException(
            "Value by ${path.contentToString()} path isn't simple, actual value: $this ${this::class.java.simpleName}, message: $this"
        )
    }
}

@Throws(FieldNotFoundException::class)
fun Map<*, *>.getList(vararg path: String): List<*>? = getField(*path)?.run {
    when (this) {
        is List<*> -> this
        else -> throw FieldNotFoundException(
            "Value by ${path.contentToString()} path isn't list, actual value: $this ${this::class.java.simpleName}, message: $this"
        )
    }
}

@Throws(FieldNotFoundException::class)
fun Map<*, *>.getMap(vararg path: String): Map<*, *>? = getField(*path)?.run {
    when (this) {
        is Map<*, *> -> this
        else -> throw FieldNotFoundException(
            "Value by ${path.contentToString()} path isn't map, actual value: $this ${this::class.java.simpleName}, message: $this"
        )
    }
}

fun Number.convertToString(): String = when (this) {
    is Byte,
    is Short,
    is Int -> toString()

    is Float,
    is Double -> BigDecimal(toString()).stripTrailingZeros().toPlainString()

    is BigDecimal -> stripTrailingZeros().toPlainString()
    else -> error("Unsupported ${this::class.simpleName} number type, value $this")
}