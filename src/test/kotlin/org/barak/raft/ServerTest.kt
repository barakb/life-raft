package org.barak.raft

import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTimeout
import org.junit.jupiter.api.Test
import java.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.seconds

@Suppress("unused")
private val logger = KotlinLogging.logger {}

@ObsoleteCoroutinesApi
@ExperimentalTime
@InternalCoroutinesApi
internal class ServerTest {
    @Test
    fun oneNodeBecomeLeader() {
        logger.info("running route test")
        assertTimeout(Duration.ofSeconds(10)) {
            runBlocking {
                InProcessTransport().use {
                    val server = Server(endpoint("barak"))
                    delay(5.seconds)
                    assertEquals(State.Leader, server.state)
                    server.close()
                }
            }
        }
    }
}