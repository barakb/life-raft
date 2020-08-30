package org.barak.raft

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.selects.selectUnbiased
import mu.KotlinLogging
import kotlin.coroutines.CoroutineContext
import kotlin.math.floor
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
    private var term: Long = 1

    private var votedFor: String? = null

    // server that voted for me.
    private val votedGranted = mutableMapOf<String, Boolean>()

    // for each member what is the known match index with me.
    private val matchIndex = mutableMapOf<String, Int>()

    // for each member what is the next index
    private val nextIndex = mutableMapOf<String, Int>()

    @Suppress("unused")
    private val commitIndex = 0

    @Suppress("unused")
    private val lastApplied = 0

    @Suppress("unused")
    private val peers = mutableListOf<String>()

    @Suppress("unused")
    private val log = Log()
    private val eventChannel = Channel<Timeout>(1)
    private val alarm = Alarm(eventChannel)
    private val supervisor = SupervisorJob()

    private val job = run()

    var state: State = State.Follower


    private suspend fun becomeFollower() {
        state = State.Follower
        alarm.cancel()
        alarm.startHeartbeatDueClock()
    }

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
        when (timeout) {
            Timeout.HeartbeatDue -> startNewElection()
            Timeout.Election -> becomeLeader()
            Timeout.SendHeartbeats -> sendAppendEntries()
        }

    }

    private fun startNewElection() {
        if (state == State.Follower || state == State.Candidate) {
            logger.info("starting new election")
            term += 1
            votedFor = endpoint.name
            state = State.Candidate
            votedGranted.clear()
            matchIndex.clear()
            sendRequestVotes()
            alarm.startElectionClock()
        }
    }

    private fun sendRequestVotes() {
        if (state == State.Candidate) {
            logger.info("sendRequestVotes")
            peers.forEach {
                sendRequestVote(it)
            }
        }
    }

    private fun sendRequestVote(to: String) {
        send(Message.RequestVote(endpoint.name, to, term))
    }

    private fun becomeLeader() {
        if (state == State.Candidate) {
            val votedForMe = votedGranted.values.filter { it }.size + 1
            if (floor((peers.size + 1.0) / 2.0) < votedForMe) {
                logger.info("server ${endpoint.name} become leader of term $term")
                state = State.Leader
                alarm.startLeaderAlarm()
            }
        }
    }

    private fun sendAppendEntries() {
        logger.info("sending appendEntries :)")
    }

    @Suppress("unused")
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
                becomeFollower()
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
                alarm.cancel()
                logger.info("done")
            }
        }
    }

    private suspend fun select(): SelectResult {
        return selectUnbiased {
            eventChannel.onReceiveOrClosed {
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