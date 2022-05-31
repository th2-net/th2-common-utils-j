package com.exactpro.th2.common.utils.event

import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.AnyMessageOrBuilder
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupOrBuilder
import com.exactpro.th2.common.grpc.RawMessage

fun Message.toGroup(): MessageGroup = MessageGroup.newBuilder().add(this).build()
fun RawMessage.toGroup(): MessageGroup = MessageGroup.newBuilder().add(this).build()
fun AnyMessage.toGroup(): MessageGroup = MessageGroup.newBuilder().addMessages(this).build()

val AnyMessageOrBuilder.sessionAlias: String
    get() = when {
        hasMessage() -> message.metadata.id.connectionId.sessionAlias
        hasRawMessage() -> message.metadata.id.connectionId.sessionAlias
        else -> error("Unsupported message kind: $kindCase")
    }

fun MessageGroup.Builder.add(message: Message): MessageGroup.Builder = apply { addMessagesBuilder().message = message }
fun MessageGroup.Builder.add(message: RawMessage): MessageGroup.Builder = apply { addMessagesBuilder().rawMessage = message }

val MessageGroupOrBuilder.sessionAlias: String?
    get() {
        messagesList.forEach {
            with(it.sessionAlias) {
                if (isNotBlank()) {
                    return this
                }
            }
        }
        return null
    }