// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.iroh

import computer.iroh.BiStream
import computer.iroh.Connection
import computer.iroh.Endpoint
import computer.iroh.EndpointAddr
import computer.iroh.EndpointId
import computer.iroh.EndpointOptions
import computer.iroh.RelayMode
import computer.iroh.presetN0
import computer.iroh.presetN0DisableRelay
import farm.query.vgirpc.transport.IrohEndpoint
import farm.query.vgirpc.transport.IrohDispatchCertainty
import farm.query.vgirpc.transport.IrohErrorCategory
import farm.query.vgirpc.transport.IrohErrorStage
import farm.query.vgirpc.transport.IrohTransportOptions
import farm.query.vgirpc.transport.IrohTransportException
import farm.query.vgirpc.transport.IrohTransportProvider
import farm.query.vgirpc.transport.RpcTransport
import java.io.InputStream
import java.io.OutputStream
import java.security.SecureRandom
import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.withTimeout

/** Native provider backed by the official `computer.iroh:iroh` binding. */
class OfficialIrohTransportProvider : IrohTransportProvider {
    private companion object {
        val processSecret = ByteArray(32).also { SecureRandom().nextBytes(it) }
    }

    override fun openArrowMux(endpoint: IrohEndpoint, options: IrohTransportOptions): RpcTransport {
        if (endpoint.scheme() != IrohEndpoint.Scheme.IROH) {
            throw IrohTransportException(
                "raw transport requires iroh://",
                IrohErrorStage.BIND,
                IrohErrorCategory.UNSUPPORTED,
                IrohDispatchCertainty.NOT_SENT,
            )
        }
        return runBlocking {
            val deadline = System.nanoTime() + options.connectTimeout().toNanos()
            fun remainingMillis(): Long =
                ((deadline - System.nanoTime()) / 1_000_000L).coerceAtLeast(1L)
            val relayMode = when {
                options.noRelay() -> RelayMode.disabled()
                options.relayUrls().isNotEmpty() -> RelayMode.customFromUrls(options.relayUrls())
                else -> null
            }
            val local = try {
                withTimeout(remainingMillis()) {
                    Endpoint.bind(
                    EndpointOptions(
                        preset = if (options.noRelay()) presetN0DisableRelay() else presetN0(),
                        secretKey = options.secretKey() ?: processSecret.copyOf(),
                        alpns = listOf(IrohEndpoint.ARROW_MUX_ALPN.toByteArray(Charsets.UTF_8)),
                        relayMode = relayMode,
                    ),
                )
                }
            } catch (error: Throwable) {
                throw structured(error, IrohErrorStage.BIND, IrohDispatchCertainty.NOT_SENT)
            }
            try {
                val connection = try {
                    val remoteId = EndpointId.fromBytes(endpoint.endpointIdBytes())
                    withTimeout(remainingMillis()) {
                        local.connect(
                            EndpointAddr(remoteId, options.remoteRelayUrl(), options.directAddresses()),
                            IrohEndpoint.ARROW_MUX_ALPN.toByteArray(Charsets.UTF_8),
                        )
                    }
                } catch (error: Throwable) {
                    throw structured(error, IrohErrorStage.CONNECT, IrohDispatchCertainty.NOT_SENT)
                }
                val stream = try {
                    withTimeout(remainingMillis()) { connection.openBi() }
                } catch (error: Throwable) {
                    connection.close(0, byteArrayOf())
                    connection.close()
                    throw structured(error, IrohErrorStage.OPEN_STREAM, IrohDispatchCertainty.NOT_SENT)
                }
                OfficialTransport(local, connection, stream, options)
            } catch (error: Throwable) {
                local.shutdown()
                local.close()
                throw error
            }
        }
    }

    override fun supportsHttp(): Boolean = false
}

private fun structured(
    error: Throwable,
    stage: IrohErrorStage,
    certainty: IrohDispatchCertainty,
): IrohTransportException {
    if (error is IrohTransportException) return error
    val cancelled = error is CancellationException && error !is TimeoutCancellationException
    val category = when {
        cancelled -> IrohErrorCategory.CANCELLED
        error is TimeoutCancellationException -> IrohErrorCategory.TIMEOUT
        error is LinkageError -> IrohErrorCategory.UNSUPPORTED
        stage == IrohErrorStage.READ || stage == IrohErrorStage.WRITE ->
            IrohErrorCategory.CONNECTION_RESET
        else -> IrohErrorCategory.UNAVAILABLE
    }
    return IrohTransportException(
        error.message ?: "native Iroh operation failed",
        if (cancelled) IrohErrorStage.CANCEL else stage,
        category,
        certainty,
        error,
    )
}

private class OfficialTransport(
    private val endpoint: Endpoint,
    private val connection: Connection,
    private val stream: BiStream,
    private val options: IrohTransportOptions,
) : RpcTransport {
    private val closed = AtomicBoolean()
    private val input = IrohInput(stream, options)
    private val output = IrohOutput(stream, options)

    override fun reader(): InputStream = input
    override fun writer(): OutputStream = output

    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        runBlocking {
            runCatching { withTimeout(options.ioTimeout().toMillis()) { stream.send().finish() } }
            runCatching { stream.recv().stop(0uL) }
            runCatching { connection.close(0, byteArrayOf()) }
            runCatching { endpoint.shutdown() }
        }
        stream.close()
        connection.close()
        endpoint.close()
    }
}

private class IrohInput(private val stream: BiStream, private val options: IrohTransportOptions) : InputStream() {
    override fun read(): Int {
        val one = ByteArray(1)
        return if (read(one, 0, 1) < 0) -1 else one[0].toInt() and 0xff
    }

    override fun read(buffer: ByteArray, offset: Int, length: Int): Int {
        if (length == 0) return 0
        val bytes = try {
            runBlocking {
                withTimeout(options.ioTimeout().toMillis()) {
                    stream.recv().read(minOf(length, 64 shl 20).toUInt())
                }
            }
        } catch (error: Throwable) {
            throw structured(error, IrohErrorStage.READ, IrohDispatchCertainty.SENT)
        }
        if (bytes.isEmpty()) return -1
        bytes.copyInto(buffer, offset)
        return bytes.size
    }
}

private class IrohOutput(private val stream: BiStream, private val options: IrohTransportOptions) : OutputStream() {
    override fun write(value: Int) = write(byteArrayOf(value.toByte()))

    override fun write(buffer: ByteArray, offset: Int, length: Int) {
        if (length == 0) return
        val owned = buffer.copyOfRange(offset, offset + length)
        try {
            runBlocking {
                withTimeout(options.ioTimeout().toMillis()) { stream.send().writeAll(owned) }
            }
        } catch (error: Throwable) {
            throw structured(error, IrohErrorStage.WRITE, IrohDispatchCertainty.UNKNOWN)
        }
    }
}
