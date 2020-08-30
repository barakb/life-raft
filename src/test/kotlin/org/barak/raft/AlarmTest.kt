package org.barak.raft

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@Suppress("unused")
private val logger = KotlinLogging.logger {}

internal class AlarmTest {

    @Test
    fun testSetAlarm() {
        Assertions.assertTimeout(Duration.ofSeconds(2)) {
            runBlocking {
                val sendChannel = Channel<Timeout>(10)
                Alarm(sendChannel).use {
                    setLeaderAlarm()
                    val event = sendChannel.receive()
                    assertEquals(Timeout.Leader, event)
                }
            }
        }
    }

    @ExperimentalCoroutinesApi
    @Test
    fun testSetCancelAlarm() {
        Assertions.assertTimeout(Duration.ofSeconds(2)) {
            runBlocking {
                val sendChannel = Channel<Timeout>(10)
                Alarm(sendChannel).use {
                    setLeaderAlarm()
                    cancelAlarm()
                    delay(1003)
                    assertTrue(sendChannel.isEmpty)
                }
            }
        }
    }
}