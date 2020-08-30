package org.barak.raft

sealed class Message(open val from: String, open val to: String) {
    data class RequestVote(override val from: String, override val to: String, val term: Long) : Message(from, to)
    class RequestVoteRsp(from: String, to: String) : Message(from, to)
    class AppendEntries(from: String, to: String) : Message(from, to)
    class AppendEntriesRsp(from: String, to: String) : Message(from, to)
}

sealed class Timeout {
    object Election : Timeout()
}

sealed class SelectResult {
    data class M(val message: Message) : SelectResult()
    data class T(val timeout: Timeout) : SelectResult()
    object Closed : SelectResult()
}