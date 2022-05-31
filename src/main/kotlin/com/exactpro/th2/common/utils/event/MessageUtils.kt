package com.exactpro.th2.common.utils.event

import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupOrBuilder
import com.exactpro.th2.common.grpc.RawMessage

fun Message.toGroup(): MessageGroup = MessageGroup.newBuilder().addMessage(this).build()
fun RawMessage.toGroup(): MessageGroup = MessageGroup.newBuilder().addMessage(this).build()

fun MessageGroup.Builder.addMessage(message: Message): MessageGroup.Builder = this.addMessages(AnyMessage.newBuilder().setMessage(message).build())
fun MessageGroup.Builder.addMessage(message: RawMessage): MessageGroup.Builder = this.addMessages(AnyMessage.newBuilder().setRawMessage(message).build())

fun MessageGroupOrBuilder.getSessionAlias(): String? {
    messagesList.forEach {
        when {
            it.hasMessage() && !it.message.metadata.id.connectionId.sessionAlias.isNullOrBlank() -> return it.message.metadata.id.connectionId.sessionAlias
            it.hasRawMessage() && !it.message.metadata.id.connectionId.sessionAlias.isNullOrBlank() -> return it.message.metadata.id.connectionId.sessionAlias
        }
    }
    return null
}