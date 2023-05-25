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

import com.exactpro.th2.common.event.bean.TreeTable
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageID


interface MessageWrapper {
    val id: MessageID
    val parentEventId: EventID?
    val messageType: String
    val protocol: String
    val properties: Map<String, String>

    val protoMessage: Message
    val treeTable: TreeTable

    /**
     * Traverses the internal message and returns simple value by [path]
     * @return null when the last element exist but has null value otherwise return [String] value
     * @throws [FieldNotFoundException] if message doesn't include full path or message structure doesn't match to path or the target field isn't simple type
     */
    @Throws(FieldNotFoundException::class)
    fun getSimple(vararg path: String): String?

    companion object {
        val MessageWrapper.sessionAlias: String?
            get() = id.sessionAlias
    }
}