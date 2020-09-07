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

    fun prevIndex(): Long {
        return lastIndex() - 1
    }

    fun prevTerm(): Long {
        return if (prevIndex() < 0) {
            0
        } else {
            entries[prevIndex().toInt()].term
        }
    }

    operator fun get(index: Long): LogEntry? {
        return if ((0 < index) && (index <= entries.size.toLong())) {
            entries[index.toInt() - 1]
        } else {
            null
        }
    }

    fun getTerm(index: Long): Long {
        return this[index]?.term ?: 0L
    }

    fun subList(from: Long, to: Long): List<LogEntry> {
        return if (from <= to) {
            entries.subList(from.toInt(), to.toInt())
        } else {
            listOf()
        }
    }

    fun pop() {
        if (entries.isNotEmpty()) {
            entries.removeAt(entries.lastIndex)
        }
    }

    fun push(logEntry: LogEntry) {
        entries.add(logEntry)
    }
}