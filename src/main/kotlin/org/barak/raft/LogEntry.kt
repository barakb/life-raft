package org.barak.raft

data class LogEntry(val term: Long, val value: String)