package org.barak.raft

import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import kotlin.time.ExperimentalTime
import kotlin.time.seconds

@Suppress("unused")
private val logger = KotlinLogging.logger {}

@ExperimentalTime
@ObsoleteCoroutinesApi
@InternalCoroutinesApi
fun main() = runBlocking {
    InProcessTransport().use {
        val server = Server(endpoint("barak"))
        delay(5.seconds)
        server.close()
    }
}