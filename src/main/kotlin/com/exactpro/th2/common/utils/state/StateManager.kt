package com.exactpro.th2.common.utils.state

@Suppress("unused")
interface StateManager {
    fun store(rawData: ByteArray, stateSessionAlias: String, bookId: String)
    fun load(stateSessionAlias: String, bookId: String): ByteArray?
}