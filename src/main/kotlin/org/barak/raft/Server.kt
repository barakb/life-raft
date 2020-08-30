package org.barak.raft

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.selects.selectUnbiased
import mu.KotlinLogging
import kotlin.coroutines.CoroutineContext
import kotlin.time.ExperimentalTime


@Suppress("unused")
enum class State {
    Follower,
    Candidate,
    Leader
}

@Suppress("unused")
private val logger = KotlinLogging.logger {}

@ObsoleteCoroutinesApi
@ExperimentalTime
@InternalCoroutinesApi
class Server(
    private val endpoint: Endpoint
) : CoroutineScope {
    @Suppress("unused")
    private val term: Long = 1

    @Suppress("unused")
    private val commitIndex = 0

    @Suppress("unused")
    private val lastApplied = 0

    @Suppress("unused")
    private val peers = mutableListOf<String>()

    @Suppress("unused")
    private val log = Log()
    private val timeouts = Channel<Timeout>(1)
    private val timeoutGenerator = TimeoutGenerator(timeouts)
    private val supervisor = SupervisorJob()

    private val job = run()


    private fun handleMessage(message: Message) {
        when (message) {
            is Message.RequestVote -> TODO()
            is Message.RequestVoteRsp -> TODO()
            is Message.AppendEntries -> TODO()
            is Message.AppendEntriesRsp -> TODO()
        }
    }

    private fun handleTimeout(timeout: Timeout) {
        logger.info("handling timeout $timeout")
//        timeoutGenerator.cancel()
//        timeoutGenerator.startElectionTimeouts()
    }

    private fun send(message: Message) {
        if (!endpoint.sendChannel.offer(message)) {
            logger.error("failed to send message: $message, endpoint sendChannel is full")
        }
    }

    private fun run(): Job {
        return launch {
            logger.info("run using scope $this")
            try {
                logger.info("server running")
                timeoutGenerator.startElectionTimeouts()
                while (isActive) {
                    when (val selectResult = select()) {
                        is SelectResult.M -> handleMessage(selectResult.message)
                        is SelectResult.T -> handleTimeout(selectResult.timeout)
                        SelectResult.Closed -> {
                            logger.info("returning")
                            return@launch
                        }
                    }
                }
            } finally {
                timeoutGenerator.cancel()
                logger.info("done")
            }
        }
    }

    private suspend fun select(): SelectResult {
        return selectUnbiased {
            timeouts.onReceiveOrClosed {
                if (it.isClosed) {
                    SelectResult.Closed
                } else {
                    SelectResult.T(it.value)
                }
            }
            endpoint.receiveChannel.onReceiveOrClosed {
                if (it.isClosed) {
                    SelectResult.Closed
                } else {
                    SelectResult.M(it.value)
                }
            }
        }

    }

    suspend fun close(): Job {
        job.cancel()
        endpoint.close()
        return job
    }

    @Suppress("unused")
    suspend fun use(block: suspend Server.() -> Unit) {
        block().also { close().join() }
    }


    override val coroutineContext: CoroutineContext
        get() = Dispatchers.Default + CoroutineName("server:${endpoint.name}") + supervisor
}