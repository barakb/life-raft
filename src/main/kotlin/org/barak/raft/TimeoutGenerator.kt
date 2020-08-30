package org.barak.raft

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.SendChannel
import mu.KotlinLogging
import kotlin.coroutines.CoroutineContext
import kotlin.random.Random

@Suppress("unused")
private val logger = KotlinLogging.logger {}

class TimeoutGenerator(private val sendChannel: SendChannel<Timeout>) : CoroutineScope {
    private val supervisor = SupervisorJob()


    fun startElectionTimeouts() {
        sendTimeouts(150, 300, Timeout.Election)
    }

    private fun sendTimeouts(from: Long, to: Long, timeout: Timeout): Job {
        return launch {
            while (isActive) {
                delay(Random.nextLong(from, to))
                sendChannel.send(timeout)
            }
        }
    }

    suspend fun cancel() {
        supervisor.children.forEach {
            it.cancel()
            it.join()
        }
        logger.info("cancelChildren $supervisor")
    }

    override val coroutineContext: CoroutineContext
        get() = Dispatchers.Default + CoroutineName("timeout generator")
}