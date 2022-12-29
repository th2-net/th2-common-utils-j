package com.exactpro.th2.common.utils.state

@Suppress("unused")
interface StateManager {
    fun store(rawData: ByteArray, stateSessionAlise: String)
    fun load(stateSessionAlise: String, bookId: String): ByteArray?
}