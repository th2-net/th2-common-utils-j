package com.exactpro.th2.common.utils.state

@Suppress("unused")
interface StateManager {
    fun store(rawData: ByteArray, stateSessionAlias: String, bookName: String)
    fun load(stateSessionAlias: String, bookName: String): ByteArray?
}