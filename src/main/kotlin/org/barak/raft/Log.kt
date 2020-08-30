package org.barak.raft

data class Log(val entries: MutableList<LogEntry> = mutableListOf()) {
    fun lastTerm(): Long {
        return if (entries.isEmpty()) {
            0
        } else {
            entries[entries.size - 1].term
        }
    }

    fun lastIndex(): Long {
        return entries.size.toLong()
    }
}