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

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.atLeastOnce
import org.mockito.kotlin.clearInvocations
import org.mockito.kotlin.mock
import org.mockito.kotlin.same
import org.mockito.kotlin.timeout
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoMoreInteractions

class DelaySenderTest {

    private val onOverflow: () -> Unit = mock { }
    private val onEmpty: () -> Unit = mock { }
    private val onNegative: (Message) -> Unit = mock { }
    private val onSend: (Message) -> Unit = mock { }

    private lateinit var delaySender: DelaySender<Message>

    @BeforeEach
    fun beforeEach() {
        delaySender = DelaySender(SIZE, Message::timeout, onOverflow, onEmpty, onNegative, onSend)
        verify(onEmpty, timeout(1_000)).invoke()
        clearInvocations(onEmpty)
    }

    @Test
    fun `queue overflow`() {
        val message = Message(100)
        delaySender.put(message)
        delaySender.put(message)

        verify(onOverflow).invoke()
        verify(onSend, timeout(200).times(2)).invoke(same(message))
        verify(onEmpty, atLeastOnce()).invoke()
        /*
          possible cases:
           - sender: take (wait)
             main: put
             sender: sleep
             main: overflow
             main: put
             sender: dry (1)
             sender: take (on event)
           - sender: take (wait)
             main: put
             sender: sleep
             main: overflow
             sender: dry (0)
             sender: take (on event)
             main: put
             sender: dry
             sender: take (on event)
         */
    }

    @Test
    fun `positive timeout`() {
        val message = Message(100)
        delaySender.put(message)
        verify(onSend, timeout(100)).invoke(same(message))
        verify(onEmpty).invoke()
    }

    @Test
    fun `zero timeout`() {
        val message = Message(0)
        delaySender.put(message)
        verify(onSend).invoke(same(message))
        verify(onEmpty).invoke()
    }

    @Test
    fun `null timeout`() {
        val message = Message(null)
        delaySender.put(message)
        verify(onSend).invoke(same(message))
        verify(onEmpty).invoke()
    }

    @Test
    fun `negative timeout`() {
        val message = Message(-1)
        delaySender.put(message)

        verify(onNegative).invoke(same(message))
        verify(onSend).invoke(same(message))
        verify(onEmpty).invoke()
    }

    @AfterEach
    fun afterEach() {
        delaySender.close()
        verifyNoMoreInteractions(onOverflow)
        verifyNoMoreInteractions(onEmpty)
        verifyNoMoreInteractions(onNegative)
        verifyNoMoreInteractions(onSend)
    }

    companion object {
        private const val SIZE = 1
        private class Message(val timeout: Long?)
    }
}