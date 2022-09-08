package com.exactpro.th2.common.utils.message

import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.AnyMessageOrBuilder
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupOrBuilder
import com.exactpro.th2.common.grpc.RawMessage
import com.google.protobuf.util.JsonFormat

fun Message.toGroup(): MessageGroup = MessageGroup.newBuilder().add(this).build()
fun RawMessage.toGroup(): MessageGroup = MessageGroup.newBuilder().add(this).build()
fun AnyMessage.toGroup(): MessageGroup = MessageGroup.newBuilder().addMessages(this).build()

val Message.direction
    get(): Direction = metadata.id.direction
var Message.Builder.direction
    get(): Direction = metadata.id.direction
    set(value) {
        metadataBuilder.idBuilder.direction = value
    }

val RawMessage.direction
    get(): Direction = metadata.id.direction
var RawMessage.Builder.direction
    get(): Direction = metadata.id.direction
    set(value) {
        metadataBuilder.idBuilder.direction = value
    }

val AnyMessage.direction
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

val AnyMessageOrBuilder.sessionAliasOrNull: String?
    get() = when {
        hasMessage() -> message.sessionAliasOrNull
        hasRawMessage() -> rawMessage.sessionAliasOrNull
        else -> error("Unsupported message kind: $kindCase")
    }

val Message.sessionAliasOrNull: String?
    get() = sessionAlias.ifEmpty { null }

val RawMessage.sessionAliasOrNull: String?
    get() = sessionAlias.ifEmpty { null }

val MessageGroupOrBuilder.sessionAliasOrNull: String?
    get() = sessionAlias.ifEmpty { null }

val AnyMessageOrBuilder.sessionAlias: String
    get() = when {
        hasMessage() -> message.sessionAlias
        hasRawMessage() -> rawMessage.sessionAlias
        else -> error("Unsupported message kind: $kindCase")
    }

val Message.sessionAlias: String
    get() = metadata.id.connectionId.sessionAlias
var Message.Builder.sessionAlias
    get(): String = metadata.id.connectionId.sessionAlias
    set(value) {
        metadataBuilder.idBuilder.connectionIdBuilder.sessionAlias = value
    }

val RawMessage.sessionAlias: String
    get() = metadata.id.connectionId.sessionAlias
var RawMessage.Builder.sessionAlias
    get(): String = metadata.id.connectionId.sessionAlias
    set(value) {
        metadataBuilder.idBuilder.connectionIdBuilder.sessionAlias = value
    }

val MessageGroupOrBuilder.sessionAlias: String
    get() {
        var sessionAlias: String? = null
        for (message in messagesList) {
            when (sessionAlias) {
                null -> sessionAlias = message.sessionAlias
                else -> require(sessionAlias == message.sessionAlias) { "Group contains more than one session alias: ${JsonFormat.printer().print(this)}" }
            }
        }
        return sessionAlias ?: ""
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