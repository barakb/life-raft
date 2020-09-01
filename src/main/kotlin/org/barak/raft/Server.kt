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
    private val endpoint: Endpoint,
    val peers: MutableList<String> = mutableListOf()
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
    private val log = Log()
    private val eventChannel = Channel<Timeout>(1)
    private val alarm = Alarm(eventChannel, endpoint.name)
    private val supervisor = SupervisorJob()

    private val job = run()

    var state: State = State.Follower


    private suspend fun stepDown(term: Long) {
        logger.debug("${endpoint.name}: stepping down")
        this.term = term
        state = State.Follower
        votedFor = null
        alarm.setElectionAlarm()
    }


    private suspend fun handleMessage(message: Message) {
        when (message) {
            is Message.RequestVote -> handleRequestVote(message)
            is Message.RequestVoteReply -> handleRequestVoteReply(message)
            is Message.AppendEntries -> handleAppendEntries(message)
            is Message.AppendEntriesRsp -> TODO()
        }
    }

    private suspend fun handleAppendEntries(request: Message.AppendEntries) {
        logger.debug("${endpoint.name}: handling handleAppendEntries message $request")
        if (term < request.term) {
            stepDown(request.term)
        }
        if (term == request.term) {
            state = State.Follower
            alarm.setElectionAlarm()
            //todo handle log
        }
    }

    private suspend fun handleRequestVoteReply(reply: Message.RequestVoteReply) {
        logger.debug("${endpoint.name}: handling requestVoteReply message $reply")
        if (term < reply.term) {
            stepDown(reply.term)
        }
        if (state == State.Candidate && reply.term == term) {
            votedGranted[reply.from] = reply.granted
        }
        //should I become a leader ?
        becomeLeader()
    }

    private suspend fun handleRequestVote(request: Message.RequestVote) {
        logger.debug("${endpoint.name}: handling requestVote message $request")
        if (term < request.term) {
            stepDown(request.term)
        }
        var granted = false
        if (term == request.term
            && (votedFor == null || votedFor == request.from)
            && ((log.lastTerm() < request.lastLogTerm)
                    ||
                    ((log.lastTerm() == request.lastLogTerm) && (log.lastIndex() <= request.lastLogIndex)))
        ) {
            granted = true
            votedFor = request.from
        }
        send(Message.RequestVoteReply(endpoint.name, request.from, term, granted))
    }

    private suspend fun handleTimeout(timeout: Timeout) {
        logger.debug("${endpoint.name}: handling timeout $timeout")
        when (timeout) {
            Timeout.Election -> handleTimeout()
            Timeout.Leader -> sendAppendEntries()
        }

    }

    private suspend fun handleTimeout() {
        if (!becomeLeader()) {
            startNewElection()
        }
    }

    private suspend fun startNewElection() {
        if (state == State.Follower || state == State.Candidate) {
            logger.debug("${endpoint.name}: starting new election at term ${term + 1}")
            term += 1
            votedFor = endpoint.name
            state = State.Candidate
            votedGranted.clear()
            matchIndex.clear()
            sendRequestVotes()
            alarm.setElectionAlarm()
        }
    }

    private fun sendRequestVotes() {
        if (state == State.Candidate) {
            logger.debug("${endpoint.name}: sendRequestVotes")
            peers.forEach {
                sendRequestVote(it)
            }
        }
    }

    private fun sendRequestVote(to: String) {
        send(Message.RequestVote(endpoint.name, to, term, log.lastTerm(), log.lastIndex()))
    }

    private suspend fun becomeLeader(): Boolean {
        if (state == State.Candidate) {
            val votedForMe = votedGranted.values.filter { it }.size + 1
            logger.debug("${endpoint.name}: has $votedForMe votes in term $term")
            if (floor((peers.size + 1.0) / 2.0) < votedForMe) {
                logger.info("${endpoint.name}: become leader of term $term")
                state = State.Leader
                sendAppendEntries()
                return true
            }
        }
        return false
    }

    private suspend fun sendAppendEntries() {
        logger.debug("${endpoint.name}: sending appendEntries term is $term")
        if (state == State.Leader) {
            peers.forEach {
                val request = Message.AppendEntries(endpoint.name, it, term)
                send(request)
            }
            alarm.setLeaderAlarm()
        }
    }

    @Suppress("unused")
    private fun send(message: Message) {
        if (!endpoint.sendChannel.offer(message)) {
            logger.error("${endpoint.name}: failed to send message: $message, endpoint sendChannel is full")
        } else {
            logger.debug("${endpoint.name}: sent -> $message")
        }
    }

    private fun run(): Job {
        return launch {
            try {
                stepDown(term)
                while (isActive) {
                    when (val selectResult = select()) {
                        is SelectResult.M -> handleMessage(selectResult.message)
                        is SelectResult.T -> handleTimeout(selectResult.timeout)
                        SelectResult.Closed -> {
                            return@launch
                        }
                    }
                }
            } finally {
                alarm.cancel()
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