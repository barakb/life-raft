package org.barak.raft

sealed class Message(open val from: String, open val to: String) {
    data class RequestVote(
        override val from: String, override val to: String,
        val term: Long, val lastLogTerm: Long, val lastLogIndex: Long
    ) : Message(from, to)

    data class RequestVoteReply(
        override val from: String, override val to: String,
        val term: Long, val granted: Boolean
    ) : Message(from, to)

    data class AppendEntries(
        override val from: String, override val to: String,
        val term: Long,
        val leaderId: String,
        val prevLogIndex: Long,
        val prevLogTerm: Long,
        val entries: List<LogEntry>,
        val leaderCommit: Long
    ) : Message(from, to)

    data class AppendEntriesReply(
        override val from: String, override val to: String,
        val term: Long,
        val success: Boolean,
        val matchIndex: Long
    ) : Message(from, to)
}

sealed class Timeout {
    object Leader : Timeout()
    object Election : Timeout()
}

sealed class SelectResult {
    data class M(val message: Message) : SelectResult()
    data class T(val timeout: Timeout) : SelectResult()
    object Closed : SelectResult()
}