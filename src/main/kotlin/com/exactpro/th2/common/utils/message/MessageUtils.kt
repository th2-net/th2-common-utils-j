package com.exactpro.th2.common.utils.message

import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.AnyMessageOrBuilder
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupOrBuilder
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.MessageOrBuilder
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.grpc.RawMessageOrBuilder
import com.google.protobuf.Duration
import com.google.protobuf.Timestamp
import com.google.protobuf.util.JsonFormat
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.Calendar
import java.util.Date
import java.util.TimeZone

typealias JavaDuration = java.time.Duration

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
            hasRawMessage() -> rawMessageBuilder.direction  = value
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
                else -> require(sessionAlias == message.sessionAlias) { "Group contains more than one session alias: ${JsonFormat.printer().print(this)}" }
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
                else -> require(direction == message.direction) { "Group contains more than one direction: ${JsonFormat.printer().print(this)}" }
            }
        }
        return direction ?: Direction.UNRECOGNIZED
    }

fun MessageGroup.Builder.add(message: Message): MessageGroup.Builder = apply { addMessagesBuilder().message = message }
fun MessageGroup.Builder.add(message: RawMessage): MessageGroup.Builder = apply { addMessagesBuilder().rawMessage = message }

fun Instant.toTimestamp(): Timestamp = Timestamp.newBuilder().setSeconds(epochSecond).setNanos(nano).build()
fun Date.toTimestamp(): Timestamp = toInstant().toTimestamp()
fun LocalDateTime.toTimestamp(zone: ZoneOffset) : Timestamp = toInstant(zone).toTimestamp()
fun LocalDateTime.toTimestamp() : Timestamp = toTimestamp(ZoneOffset.of(TimeZone.getDefault().id))
fun Calendar.toTimestamp() : Timestamp = toInstant().toTimestamp()
fun Duration.toJavaDuration(): JavaDuration = JavaDuration.ofSeconds(seconds, nanos.toLong())
fun JavaDuration.toProtoDuration(): Duration = Duration.newBuilder().setSeconds(seconds).setNanos(nano).build()
