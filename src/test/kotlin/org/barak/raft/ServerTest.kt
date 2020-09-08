package org.barak.raft

import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.RepeatedTest
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
        assertTimeout(Duration.ofSeconds(5)) {
            runBlocking {
                InProcessTransport().use {
                    val server = Server(endpoint("barak"))
                    delay(3.seconds)
                    assertEquals(State.Leader, server.state)
                    server.close()
                }
            }
        }
    }

    @RepeatedTest(1)
    fun twoNodesSelectALeaderLeaderEmptyLog() {
        assertTimeout(Duration.ofSeconds(5)) {
            runBlocking {
                InProcessTransport().use {
                    val server1 = Server(endpoint("server1"), mutableListOf("server2"))
                    val server2 = Server(endpoint("server2"), mutableListOf("server1"))
                    delay(3.seconds)
                    val servers = listOf(server1, server2)
                    assertTrue(exactlyOneIsLeader(servers))
                    val leaderLog = servers.find { it.state == State.Leader }!!.log
                    assertTrue(servers.map { it.log }.all { it == leaderLog })

                    server1.close()
                    server2.close()
                }
            }
        }
    }

    @RepeatedTest(5)
    fun twoNodesSelectALeaderLeaderWithSimpleLogsReplication() {
        assertTimeout(Duration.ofSeconds(5)) {
            runBlocking {
                InProcessTransport().use {
                    val log = Log()
                    log.push(LogEntry(0, "foo"))
                    val server1 = Server(endpoint("server1"), mutableListOf("server2"), log)
                    val server2 = Server(endpoint("server2"), mutableListOf("server1"))
                    delay(3.seconds)
                    assertTrue(server1.state == State.Leader)
                    assertTrue(server2.log.lastIndex() == 1L)
                    assertTrue(server2.log[1L] == log[1L])
                    server1.close()
                    server2.close()
                }
            }
        }
    }

    @RepeatedTest(5)
    fun twoNodesSelectALeaderLeaderWithComplexLogsReplication() {
        assertTimeout(Duration.ofSeconds(5)) {
            runBlocking {
                InProcessTransport().use {
                    val log1 = Log()
                    log1.push(LogEntry(0, "term0"))
                    log1.push(LogEntry(1, "term1"))
                    log1.push(LogEntry(1, "term1"))
                    val log2 = Log()
                    log2.push(LogEntry(0, "term0"))
                    log2.push(LogEntry(0, "term0"))
                    log2.push(LogEntry(0, "term0"))
                    log2.push(LogEntry(0, "term0"))
                    val server1 = Server(endpoint("server1"), mutableListOf("server2"), log1)
                    val server2 = Server(endpoint("server2"), mutableListOf("server1"), log2)
                    delay(3.seconds)
                    assertTrue(server1.state == State.Leader)
                    assertEquals(server1.log, server2.log)
                    server1.close()
                    server2.close()
                }
            }
        }
    }

    @RepeatedTest(5)
    fun threeNodesSelectALeaderLeader() {
        assertTimeout(Duration.ofSeconds(5)) {
            runBlocking {
                InProcessTransport().use {
                    val server1 = Server(endpoint("server1"), mutableListOf("server2", "server3"))
                    val server2 = Server(endpoint("server2"), mutableListOf("server1", "server3"))
                    val server3 = Server(endpoint("server3"), mutableListOf("server1", "server2"))
                    delay(3.seconds)
                    val servers = listOf(server1, server2, server3)
                    assertTrue(exactlyOneIsLeader(servers))
                    val leaderLog = servers.find { it.state == State.Leader }!!.log
                    assertTrue(servers.map { it.log }.all { it == leaderLog })
                    server1.close()
                    server2.close()
                    server3.close()
                }
            }
        }
    }

    private fun exactlyOneIsLeader(servers: List<Server>): Boolean {
        return (servers.filter { it.state == State.Leader }
            .count() == 1) && (servers.filter { it.state == State.Follower }.count() == servers.size - 1)
    }

}