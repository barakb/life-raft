package org.barak.raft

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import mu.KotlinLogging
import kotlin.coroutines.CoroutineContext
import kotlin.random.Random

@Suppress("unused")
private val logger = KotlinLogging.logger {}

class Alarm(private val sendChannel: Channel<Timeout>, private val name: String = "alarm") : CoroutineScope {

    private val cmdChannel = Channel<() -> Unit>()
    private var term = 0
    private var job: Job? = null
    private val supervisor = SupervisorJob()


    init {
        start()
    }

    suspend fun setCandidateAlarm() {
        job?.cancel()
        cmdChannel.send { sendTimeout(150, 300, Timeout.Election) }
    }

    suspend fun setLeaderAlarm() {
        job?.cancel()
        cmdChannel.send { sendTimeout(100, Timeout.Leader) }
    }

    suspend fun setFollowerAlarm() {
        job?.cancel()
        cmdChannel.send { sendTimeout(150, 300, Timeout.Follower) }
    }

    suspend fun cancelAlarm() {
        job?.cancel()
        cmdChannel.send {
            job = null
            term += 1
            var next = sendChannel.poll()
            while (next != null) {
                next = sendChannel.poll()
            }
        }
    }

    private fun sendTimeout(from: Long, to: Long, timeout: Timeout) {
        return sendTimeout(Random.nextLong(from, to), timeout)
    }

    private fun sendTimeout(delay: Long, timeout: Timeout) {
        job = null
        term += 1
        val myTerm = term
        logger.debug("$name: setting alarm to $timeout in $delay milliseconds myTerm = $myTerm term = $term")
        job = launch {
            delay(delay)
            if (term == myTerm) {
                logger.debug("$name: sending alarm $timeout after delay $delay, myTerm = $myTerm term = $term")
                sendChannel.send(timeout)
            }
        }
    }

    suspend fun close() {
        cmdChannel.close()
        supervisor.cancel()
        supervisor.join()
    }

    suspend fun use(block: suspend Alarm.() -> Unit) {
        try {
            block()
        } finally {
            close()
        }
    }

    private fun start() {
        launch {
            for (cmd in cmdChannel) {
                cmd()
            }
        }
    }


    override val coroutineContext: CoroutineContext
        get() = Dispatchers.Default + CoroutineName("Alarm ($name)") + supervisor
}