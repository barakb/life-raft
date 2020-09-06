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
    private var term: Long = 0

    private var votedFor: String? = null

    // server that voted for me.
    private val votedGranted = mutableMapOf<String, Boolean>()


    // for each member what is the next index, initialized to leader last log index + 1
    private val _nextIndex = mutableMapOf<String, Int>()

    // for each member what is the known match index with me, initialized to leader last log index + 1.
    private val _matchIndex = mutableMapOf<String, Int>()

    @Suppress("unused")
    private var commitIndex: Long = 0

    @Suppress("unused")
    private val lastApplied: Long = 0


    @Suppress("unused")
    private val log = Log()
    private val eventChannel = Channel<Timeout>(1)
    private val alarm = Alarm(eventChannel, endpoint.name)
    private val supervisor = SupervisorJob()

    private val job = run()

    var state: State = State.Follower


    private suspend fun stepDown(term: Long) {
        logger.debug("${endpoint.name}: stepping down")
        if (this.term != term) {
            votedFor = null
            this.term = term
        }
        state = State.Follower
        alarm.setElectionAlarm()
    }


    private suspend fun handleMessage(message: Message) {
        when (message) {
            is Message.RequestVote -> handleRequestVote(message)
            is Message.RequestVoteReply -> handleRequestVoteReply(message)
            is Message.AppendEntries -> handleAppendEntries(message)
            is Message.AppendEntriesRsp -> handleAppendEntriesRsp(message)
        }
    }

    private fun handleAppendEntriesRsp(resp: Message.AppendEntriesRsp) {
        logger.debug("handleAppendEntriesRsp $resp")
        // TODO("")
    }

    /* Reply false if request.term < term
     * Reply false if log does not contains prev entry with prev term
     * if existing log entry conflict with new one (same index different log)
     *  delete existing entry with all that follow it.
     * Append new entries if not already in the log
     * if leaderCommit >commitIndex set commitIndex = min(leaderCommit, index of last log)
     *
     */
    private suspend fun handleAppendEntries(request: Message.AppendEntries) {
        logger.debug("${endpoint.name}: handling handleAppendEntries message $request")
        if (request.term < term) {
            // ignore message from older terms
            send(Message.AppendEntriesRsp(endpoint.name, request.from, term, false, 0))
        } else if (term < request.term) {
            // update my term
            val prevTerm = term
            stepDown(request.term)
            send(Message.AppendEntriesRsp(endpoint.name, request.from, prevTerm, false, 0))
        } else {
            stepDown(request.term)
            var success = false
            var matchIndex = 0L
            // log consistency check
            if ((request.prevLogIndex == 0L) ||
                ((request.prevLogIndex <= log.lastIndex()) && (log.getTerm(request.prevLogIndex) == request.prevLogTerm))
            ) {
                success = true
                var index = request.prevLogIndex
                // update my log
                for (logEntry in request.entries) {
                    index += 1
                    if (log.getTerm(index) != logEntry.term) {
                        while (index - 1 < log.lastIndex()) {
                            log.pop()
                        }
                        log.push(logEntry)
                    }
                }
                matchIndex = index
                commitIndex = Math.max(commitIndex, request.leaderCommit)
            }
            send(Message.AppendEntriesRsp(endpoint.name, request.from, term, success, matchIndex))
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
                initializeLeaderState()
                sendAppendEntries()
                return true
            }
        }
        return false
    }

    private fun initializeLeaderState() {
        state = State.Leader
        _nextIndex.clear()
        _matchIndex.clear()
    }

    private fun nextIndex(member: String): Long {
        return _nextIndex[member]?.toLong() ?: (log.lastIndex() + 1)
    }

    private fun matchIndex(member: String): Long {
        return _matchIndex[member]?.toLong() ?: 0
    }

    /**
     * Until the leader has discovered where it and the follower’s logs match, the leader can send
     * AppendEntries with no entries (like heartbeats) to save bandwidth.
     * Then, once the matchIndex immediately precedes the nextIndex,
     * the leader should begin to send the actual entries.
     */
    private suspend fun sendAppendEntries() {
        logger.debug("${endpoint.name}: sending appendEntries term is $term")
        if (state == State.Leader) {
            peers.forEach {
                val entries = if (matchIndex(it) + 1 == nextIndex(it)) {
                    log.subList(nextIndex(it), log.lastIndex())
                } else {
                    listOf()
                }
                val request = Message.AppendEntries(
                    endpoint.name,
                    it,
                    term,
                    endpoint.name,
                    log.prevIndex() - 1,
                    log.prevTerm(),
                    entries,
                    commitIndex
                )
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