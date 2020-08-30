package org.barak.raft

import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertTimeout
import org.junit.jupiter.api.Test
import java.time.Duration.ofSeconds
import kotlin.time.ExperimentalTime

@Suppress("unused")
private val logger = KotlinLogging.logger {}

@ExperimentalTime
@InternalCoroutinesApi
internal class InProcessTransportTest {
    @Test
    fun route() {
        logger.info("running route test")
        assertTimeout(ofSeconds(2)) {
            runBlocking {
                InProcessTransport().use {
                    val endpoint1 = endpoint("endpoint1")
                    val endpoint2 = endpoint("endpoint2")
                    val message = Message.AppendEntries(endpoint1.name, endpoint2.name, 0)
                    endpoint1.sendChannel.send(message)
                    val received = endpoint2.receiveChannel.receive()
                    Assertions.assertEquals(message, received)
                }
            }
        }
    }
}