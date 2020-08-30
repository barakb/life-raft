package org.barak.raft

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.SendChannel
import mu.KotlinLogging
import kotlin.coroutines.CoroutineContext
import kotlin.random.Random

@Suppress("unused")
private val logger = KotlinLogging.logger {}

class Alarm(private val sendChannel: SendChannel<Timeout>) : CoroutineScope {
    private val supervisor = SupervisorJob()

    fun startElectionClock() {
        sendTimeout(150, 300, Timeout.Election)
    }

    fun startHeartbeatDueClock() {
        sendTimeout(1500, 3000, Timeout.HeartbeatDue)
    }

    private fun sendTimeout(from: Long, to: Long, timeout: Timeout): Job {
        return launch {
            delay(Random.nextLong(from, to))
            sendChannel.send(timeout)
        }
    }

    fun startLeaderAlarm(): Job {
        return launch {
            while (isActive) {
                delay(1000)
                sendChannel.send(Timeout.SendHeartbeats)
            }
        }
    }

    suspend fun cancel() {
        supervisor.children.forEach {
            it.cancel()
        }
        supervisor.children.forEach {
            it.join()
        }
    }

    override val coroutineContext: CoroutineContext
        get() = Dispatchers.Default + CoroutineName("timeout generator")
}