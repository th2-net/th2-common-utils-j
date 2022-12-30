package com.exactpro.th2.common.utils.state

import com.exactpro.th2.common.grpc.Direction
import java.time.Instant

@Suppress("unused")
interface StateManager {
    fun store(rawData: ByteArray, stateSessionAlise: String)
    fun load(from: Instant, to: Instant, direction: Direction, stateSessionAlise: String, bookId: String): ByteArray?
}