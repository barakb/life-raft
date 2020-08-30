package org.barak.raft

data class Log(val entries: MutableList<LogEntry> = mutableListOf())