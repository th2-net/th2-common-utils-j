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
import com.exactpro.th2.common.message.messageType
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.utils.event.transport.toProto
import com.exactpro.th2.common.utils.message.transport.getString
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
    override fun getSimple(vararg path: String): String? = source.getString(*path)

    override fun toString(): String {
        return source.toJson()
    }

    companion object {
        @JvmStatic
        val DEFAULT = ProtoMessageHolder(Message.getDefaultInstance())
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
    override fun getSimple(vararg path: String): String? = source.body.getString(*path)

    override fun toString(): String {
        return "TransportMessageWrapper(transport=$source, book='$book', sessionGroup='$sessionGroup')"
    }
}