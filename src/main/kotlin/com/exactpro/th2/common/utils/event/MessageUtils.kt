package com.exactpro.th2.common.utils.event

import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.AnyMessageOrBuilder
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupOrBuilder
import com.exactpro.th2.common.grpc.RawMessage
import com.google.protobuf.TextFormat

fun Message.toGroup(): MessageGroup = MessageGroup.newBuilder().add(this).build()
fun RawMessage.toGroup(): MessageGroup = MessageGroup.newBuilder().add(this).build()
fun AnyMessage.toGroup(): MessageGroup = MessageGroup.newBuilder().addMessages(this).build()

val AnyMessageOrBuilder.sessionAlias: String?
    get() = when {
        hasMessage() -> message.sessionAlias
        hasRawMessage() -> rawMessage.sessionAlias
        else -> error("Unsupported message kind: $kindCase")
    }

val Message.sessionAlias: String?
    get() = sessionAliasOrEmpty.run {
        when {
            isEmpty() -> null
            else -> this
        }
    }

val RawMessage.sessionAlias: String?
    get() = sessionAliasOrEmpty.run {
        when {
            isEmpty() -> null
            else -> this
        }
    }

val MessageGroupOrBuilder.sessionAlias: String?
    get() = sessionAliasOrEmpty.run {
        when {
            isEmpty() -> null
            else -> this
        }
    }

val AnyMessageOrBuilder.sessionAliasOrEmpty: String
    get() = when {
        hasMessage() -> message.sessionAliasOrEmpty
        hasRawMessage() -> rawMessage.sessionAliasOrEmpty
        else -> error("Unsupported message kind: $kindCase")
    }

val Message.sessionAliasOrEmpty: String
    get() = metadata.id.connectionId.sessionAlias

val RawMessage.sessionAliasOrEmpty: String
    get() = metadata.id.connectionId.sessionAlias

val MessageGroupOrBuilder.sessionAliasOrEmpty: String
    get() {
        val result = mutableListOf<String>()
        messagesList.forEach {
            it.sessionAlias?.run(result::add)
        }
        return when(result.size) {
            1 -> result.first()
            0 -> ""
            else -> error("Too many aliases [${result.joinToString(", ")}] was found in group of messages: ${TextFormat.shortDebugString(this)}")
        }
    }

fun MessageGroup.Builder.add(message: Message): MessageGroup.Builder = apply { addMessagesBuilder().message = message }
fun MessageGroup.Builder.add(message: RawMessage): MessageGroup.Builder = apply { addMessagesBuilder().rawMessage = message }