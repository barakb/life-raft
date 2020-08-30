package org.barak.raft

import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.junit.jupiter.api.Assertions.*
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

    @Test
    fun twoNodesSelectALeaderLeader() {
        assertTimeout(Duration.ofSeconds(15)) {
            runBlocking {
                InProcessTransport().use {
                    val server1 = Server(endpoint("server1"), mutableListOf("server2"))
                    val server2 = Server(endpoint("server2"), mutableListOf("server1"))
                    delay(10.seconds)
//                    assertEquals(State.Leader, server.state)
                    assertTrue(
                        (server1.state == State.Leader && server2.state == State.Follower)
                                ||
                                (server1.state == State.Follower && server2.state == State.Leader)
                    )
                    server1.close()
                    server2.close()
                }
            }
        }
    }
}