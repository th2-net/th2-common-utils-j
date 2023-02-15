package com.exactpro.th2.common.utils.state

import com.exactpro.th2.common.grpc.EventID

@Suppress("unused")
interface StateManager {
    fun store(parentEventId: EventID, rawData: ByteArray, stateSessionAlias: String, bookName: String)
    fun load(parentEventId: EventID, stateSessionAlias: String, bookName: String): ByteArray?
}