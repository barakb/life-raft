package org.barak.raft

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.selects.selectUnbiased
import mu.KotlinLogging
import kotlin.coroutines.CoroutineContext

data class Endpoint(
    val name: String,
    val sendChannel: SendChannel<Message>,
    private val transport: Transport,
    val receiveChannel: Channel<Message> = Channel(10),
) {
    suspend fun close() {
        transport.close(name)
    }

    @Suppress("unused")
    suspend fun use(block: Endpoint.() -> Unit) {
        try {
            block()
        } finally {
            close()
        }
    }
}

@Suppress("unused")
private val logger = KotlinLogging.logger {}

interface Transport {
    fun endpoint(name: String): Endpoint
    suspend fun close(name: String)
    suspend fun close()

    @Suppress("unused")
    suspend fun use(block: suspend Transport.() -> Unit) {
        try {
            block()
        } finally {
            close()
        }
    }
}

@Suppress("unused")
@InternalCoroutinesApi
class InProcessTransport(
    private val receiveChannel: Channel<Message> = Channel(10),
    private val cmdChannel: Channel<suspend () -> Unit> = Channel(10)
) : Transport, CoroutineScope {

    private val endpoints = mutableMapOf<String, Endpoint>()
    private val job = run()

    override fun endpoint(name: String): Endpoint {
        val ep = endpoints[name]
        return if (ep != null) {
            ep
        } else {
            val res = Endpoint(name, receiveChannel, this)
            endpoints[name] = res
            res
        }
    }

    override suspend fun close(name: String) {
        cmdChannel.send {
            endpoints[name]?.close()
            endpoints.remove(name)
        }
    }

    override suspend fun close() {
        job.cancel()
        receiveChannel.cancel()
        for (endpoint in endpoints.values) {
            endpoint.close()
        }
    }

    @InternalCoroutinesApi
    private fun run(): Job {
        return launch {
            while (isActive) {
                selectUnbiased<Unit> {
                    cmdChannel.onReceiveOrClosed {
                        if (!it.isClosed) {
                            it.value()
                        }
                    }
                    receiveChannel.onReceiveOrClosed {
                        if (!it.isClosed) {
                            routeMessage(it.value)
                        }
                    }
                }
            }
        }
    }

    private fun routeMessage(message: Message) {
        val status = endpoints[message.to]?.receiveChannel?.offer(message)
        if (status == null) {
            logger.error("failed to deliver message: $message, unknown recipient: ${message.to}")
        } else if (!status) {
            logger.error("failed to deliver message: $message, queue is full")
        } else {
            logger.debug("routeMessage: $message")
        }
    }

    override val coroutineContext: CoroutineContext
        get() = Dispatchers.Default + CoroutineName("InProcessTransport")
}
