/*
 * Copyright 2022-2023 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.event.bean.IColumn
import com.exactpro.th2.common.event.bean.TreeTable
import com.exactpro.th2.common.event.bean.TreeTableEntry
import com.exactpro.th2.common.event.bean.builder.CollectionBuilder
import com.exactpro.th2.common.event.bean.builder.RowBuilder
import com.exactpro.th2.common.event.bean.builder.TreeTableBuilder
import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.AnyMessageOrBuilder
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupOrBuilder
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.MessageIDOrBuilder
import com.exactpro.th2.common.grpc.MessageOrBuilder
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.grpc.RawMessageOrBuilder
import com.exactpro.th2.common.grpc.Value
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.transport
import com.exactpro.th2.common.util.toInstant
import com.exactpro.th2.common.utils.event.toTransport
import com.exactpro.th2.common.utils.logTimestamp
import com.google.protobuf.Duration
import com.google.protobuf.Timestamp
import com.google.protobuf.util.JsonFormat
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.Locale
import java.util.Calendar
import java.util.TimeZone
import java.util.Date

typealias JavaDuration = java.time.Duration

val MessageIDOrBuilder.subsequence: List<Int>
    get() = subsequenceList
val MessageIDOrBuilder.sessionAlias: String?
    get() = connectionId.sessionAlias.ifBlank { null }
val MessageIDOrBuilder.logId: String
    get() = "$bookName:$sessionAlias:${
        direction.toString().lowercase(Locale.getDefault())
    }:${timestamp.logTimestamp}:$sequence${subsequence.joinToString("") { ".$it" }}"

fun Message.toGroup(): MessageGroup = MessageGroup.newBuilder().add(this).build()
fun RawMessage.toGroup(): MessageGroup = MessageGroup.newBuilder().add(this).build()
fun AnyMessage.toGroup(): MessageGroup = MessageGroup.newBuilder().addMessages(this).build()
fun Message.Builder.toGroup(): MessageGroup = MessageGroup.newBuilder().add(this.build()).build()
fun RawMessage.Builder.toGroup(): MessageGroup = MessageGroup.newBuilder().add(this.build()).build()
fun AnyMessage.Builder.toGroup(): MessageGroup = MessageGroup.newBuilder().addMessages(this).build()

val MessageOrBuilder.direction
    get(): Direction = metadata.id.direction
var Message.Builder.direction
    get(): Direction = metadata.id.direction
    set(value) {
        metadataBuilder.idBuilder.direction = value
    }

val RawMessageOrBuilder.direction
    get(): Direction = metadata.id.direction
var RawMessage.Builder.direction
    get(): Direction = metadata.id.direction
    set(value) {
        metadataBuilder.idBuilder.direction = value
    }

val AnyMessageOrBuilder.direction
    get(): Direction = when {
        hasMessage() -> message.direction
        hasRawMessage() -> rawMessage.direction
        else -> error("Unsupported message kind: $kindCase")
    }
var AnyMessage.Builder.direction
    get(): Direction = when {
        hasMessage() -> message.direction
        hasRawMessage() -> rawMessage.direction
        else -> error("Unsupported message kind: $kindCase")
    }
    set(value) {
        when {
            hasMessage() -> messageBuilder.direction = value
            hasRawMessage() -> rawMessageBuilder.direction = value
            else -> error("Unsupported message kind: $kindCase")
        }

    }

val AnyMessageOrBuilder.sessionAlias: String?
    get() = when {
        hasMessage() -> message.sessionAlias
        hasRawMessage() -> rawMessage.sessionAlias
        else -> error("Unsupported message kind: $kindCase")
    }

val AnyMessageOrBuilder.timestamp: Timestamp
    get() = when {
        hasMessage() -> message.timestamp
        hasRawMessage() -> rawMessage.timestamp
        else -> error("Unsupported message kind: $kindCase")
    }

val AnyMessage.book: String?
    get() = when (kindCase) {
        AnyMessage.KindCase.MESSAGE -> message.metadata.id.bookName.ifEmpty { null }
        AnyMessage.KindCase.RAW_MESSAGE -> rawMessage.metadata.id.bookName.ifEmpty { null }
        else -> error("Unsupported message kind: $kindCase")
    }

val AnyMessageOrBuilder.sessionGroup: String?
    get() = when {
        hasMessage() -> message.sessionGroup ?: message.sessionAlias
        hasRawMessage() -> rawMessage.sessionGroup ?: rawMessage.sessionAlias
        else -> error("Unsupported message kind: $kindCase")
    }

val AnyMessageOrBuilder.id: MessageID
    get() = when {
        hasMessage() -> message.id
        hasRawMessage() -> rawMessage.id
        else -> error("Unsupported message kind: $kindCase")
    }

val AnyMessageOrBuilder.parentEventId: EventID?
    get() = when {
        hasMessage() -> if (message.hasParentEventId()) message.parentEventId else null
        hasRawMessage() -> if (rawMessage.hasParentEventId()) rawMessage.parentEventId else null
        else -> error("Unsupported message kind: $kindCase")
    }

var Message.Builder.sessionAlias: String?
    get() = metadata.id.connectionId.sessionAlias.ifEmpty { null }
    set(value) {
        metadataBuilder.idBuilder.connectionIdBuilder.sessionAlias = value
    }

val MessageOrBuilder.sessionAlias: String?
    get() = metadata.id.connectionId.sessionAlias.ifEmpty { null }

val MessageOrBuilder.timestamp: Timestamp
    get() = metadata.id.timestamp

var RawMessage.Builder.sessionAlias: String?
    get() = metadata.id.connectionId.sessionAlias.ifEmpty { null }
    set(value) {
        metadataBuilder.idBuilder.connectionIdBuilder.sessionAlias = value
    }

val MessageOrBuilder.sessionGroup: String?
    get() = metadata.id.connectionId.sessionGroup.ifEmpty { null }

val MessageOrBuilder.id: MessageID
    get() = metadata.id

var Message.Builder.sessionGroup: String?
    get() = metadata.id.connectionId.sessionGroup.ifEmpty { null }
    set(value) {
        metadataBuilder.idBuilder.connectionIdBuilder.sessionGroup = value
    }

val RawMessageOrBuilder.sessionAlias: String?
    get() = metadata.id.connectionId.sessionAlias.ifEmpty { null }

val RawMessageOrBuilder.timestamp: Timestamp
    get() = metadata.id.timestamp

val RawMessageOrBuilder.sessionGroup: String?
    get() = metadata.id.connectionId.sessionGroup.ifEmpty { null }

val RawMessageOrBuilder.id: MessageID
    get() = metadata.id

var RawMessage.Builder.sessionGroup: String?
    get() = metadata.id.connectionId.sessionGroup.ifEmpty { null }
    set(value) {
        metadataBuilder.idBuilder.connectionIdBuilder.sessionGroup = value
    }

val MessageGroupOrBuilder.sessionAlias: String?
    get() {
        var sessionAlias: String? = null
        for (message in messagesList) {
            when (sessionAlias) {
                null -> sessionAlias = message.sessionAlias
                else -> require(sessionAlias == message.sessionAlias) {
                    "Group contains more than one session alias: ${
                        JsonFormat.printer().print(this)
                    }"
                }
            }
        }
        return sessionAlias
    }

val MessageGroupOrBuilder.direction: Direction
    get() {
        var direction: Direction? = null
        for (message in messagesList) {
            when (direction) {
                null -> direction = message.direction
                else -> require(direction == message.direction) {
                    "Group contains more than one direction: ${
                        JsonFormat.printer().print(this)
                    }"
                }
            }
        }
        return direction ?: Direction.UNRECOGNIZED
    }

fun MessageGroup.Builder.add(message: Message): MessageGroup.Builder = apply { addMessagesBuilder().message = message }
fun MessageGroup.Builder.add(message: RawMessage): MessageGroup.Builder =
    apply { addMessagesBuilder().rawMessage = message }

val MessageGroupOrBuilder.parentEventIds: Sequence<EventID>
    get() = messagesList.asSequence()
        .map(AnyMessage::parentEventId)
        .filterNotNull()
        .distinct()

fun Instant.toTimestamp(): Timestamp = Timestamp.newBuilder().setSeconds(epochSecond).setNanos(nano).build()
fun Date.toTimestamp(): Timestamp = toInstant().toTimestamp()
fun LocalDateTime.toTimestamp(zone: ZoneOffset): Timestamp = toInstant(zone).toTimestamp()
fun LocalDateTime.toTimestamp(): Timestamp = toTimestamp(ZoneOffset.of(TimeZone.getDefault().id))
fun Calendar.toTimestamp(): Timestamp = toInstant().toTimestamp()
fun Duration.toJavaDuration(): JavaDuration = JavaDuration.ofSeconds(seconds, nanos.toLong())
fun JavaDuration.toProtoDuration(): Duration = Duration.newBuilder().setSeconds(seconds).setNanos(nano).build()

fun Message.toTreeTable(): TreeTable = TreeTableBuilder().apply {
    for ((key, value) in fieldsMap) {
        row(key, value.toTreeTableEntry())
    }
}.build()

private fun Value.toTreeTableEntry(): TreeTableEntry = when {
    hasNullValue() -> RowBuilder()
        .column(MessageTableColumn(null))
        .build()

    hasMessageValue() -> CollectionBuilder().apply {
        for ((key, value) in messageValue.fieldsMap) {
            row(key, value.toTreeTableEntry())
        }
    }.build()

    hasListValue() -> CollectionBuilder().apply {
        listValue.valuesList.forEachIndexed { index, nestedValue ->
            val nestedName = index.toString()
            row(nestedName, nestedValue.toTreeTableEntry())
        }
    }.build()

    hasSimpleValue() -> RowBuilder()
        .column(MessageTableColumn(simpleValue))
        .build()

    else -> error("Unsupported type, value ${toJson()}")
}

data class MessageTableColumn(val fieldValue: String?) : IColumn

fun MessageID.toTransport(): MessageId =
    MessageId(connectionId.sessionAlias, direction.transport, sequence, timestamp.toInstant(), subsequenceList)

fun Value.toTransport(): Any? = when (kindCase) {
    Value.KindCase.NULL_VALUE -> null
    Value.KindCase.SIMPLE_VALUE -> simpleValue
    Value.KindCase.MESSAGE_VALUE -> messageValue.fieldsMap.mapValues { entry -> entry.value.toTransport() }
    Value.KindCase.LIST_VALUE -> listValue.valuesList.map(Value::toTransport)
    else -> "Unsupported $kindCase kind for transformation, value: ${toJson()}"
}

fun Message.toTransportBuilder(): ParsedMessage.Builder<ParsedMessage.FromMapBuilder> = ParsedMessage.builder().apply {
    with(metadata) {
        setId(id.toTransport())
        setType(messageType)
        setProtocol(protocol)
        setMetadata(propertiesMap)
    }
    with(bodyBuilder()) {
        fieldsMap.forEach { (key, value) ->
            put(key, value.toTransport())
        }
    }
    if (hasParentEventId()) {
        setEventId(parentEventId.toTransport())
    }
}

fun Message.toTransport(): ParsedMessage = toTransportBuilder().build()

/**
 * Traverses the internal message and returns [Value] by [path]
 * @throws [FieldNotFoundException] if message doesn't include full path or message structure doesn't match to path
 */
@Throws(FieldNotFoundException::class)
fun MessageOrBuilder.getField(vararg path: String): Value? = runCatching {
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
fun MessageOrBuilder.getString(vararg path: String): String? = getField(*path)?.run {
    when (kindCase) {
        Value.KindCase.NULL_VALUE -> null
        Value.KindCase.SIMPLE_VALUE -> simpleValue
        else -> throw FieldNotFoundException("Value by ${path.contentToString()} path isn't simple, value: ${this.toJson()}, message: ${toJson()}")
    }
}

@Throws(FieldNotFoundException::class)
fun MessageOrBuilder.getList(vararg path: String): List<Value>? = getField(*path)?.run {
    when (kindCase) {
        Value.KindCase.NULL_VALUE -> null
        Value.KindCase.LIST_VALUE -> listValue.valuesList
        else -> throw FieldNotFoundException("Value by ${path.contentToString()} path isn't list value, value: ${this.toJson()}, message: ${toJson()}")
    }
}

@Throws(FieldNotFoundException::class)
fun MessageOrBuilder.getMessage(vararg path: String): Message? = getField(*path)?.run {
    when (kindCase) {
        Value.KindCase.NULL_VALUE -> null
        Value.KindCase.MESSAGE_VALUE -> messageValue
        else -> throw FieldNotFoundException("Value by ${path.contentToString()} path isn't message value, value: ${this.toJson()}, message: ${toJson()}")
    }
}