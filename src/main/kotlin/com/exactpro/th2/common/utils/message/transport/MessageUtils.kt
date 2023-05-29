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
import com.exactpro.th2.common.grpc.Direction.FIRST
import com.exactpro.th2.common.grpc.Direction.SECOND
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.message.addField
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction.INCOMING
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Message
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.utils.event.transport.toProto
import com.exactpro.th2.common.utils.message.MessageTableColumn
import com.exactpro.th2.common.value.toValue
import java.math.BigDecimal
import com.exactpro.th2.common.grpc.Message as ProtoMessage

fun Message<*>.toGroup(): MessageGroup = MessageGroup.builder().apply {
    addMessage(this@toGroup)
}.build()

fun MessageId.toProto(book: String, sessionGroup: String): MessageID = MessageID.newBuilder().also {
    it.bookName = book
    it.direction = if (direction == INCOMING) FIRST else SECOND
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

fun Number.convertToString(): String = when (this) {
    is Byte,
    is Short,
    is Int -> toString()

    is Float,
    is Double -> BigDecimal(toString()).stripTrailingZeros().toPlainString()

    is BigDecimal -> stripTrailingZeros().toPlainString()
    else -> error("Unsupported ${this::class.simpleName} number type, value $this")
}