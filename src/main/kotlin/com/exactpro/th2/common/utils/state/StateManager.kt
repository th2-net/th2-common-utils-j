package com.exactpro.th2.common.utils.state

interface StateManager {
    fun store(rawData: ByteArray, stateSessionAlise: String)
    fun load(stateSessionAlise: String): ByteArray?
}