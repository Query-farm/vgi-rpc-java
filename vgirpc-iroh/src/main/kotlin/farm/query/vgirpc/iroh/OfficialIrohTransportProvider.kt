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
import farm.query.vgirpc.transport.IrohHttpRequest
import farm.query.vgirpc.transport.IrohHttpResponse
import farm.query.vgirpc.transport.IrohHttpTransport
import farm.query.vgirpc.transport.IrohTransportOptions
import farm.query.vgirpc.transport.IrohTransportException
import farm.query.vgirpc.transport.IrohTransportProvider
import farm.query.vgirpc.transport.RpcTransport
import java.io.InputStream
import java.io.OutputStream
import java.io.ByteArrayOutputStream
import java.security.SecureRandom
import java.nio.charset.StandardCharsets
import java.util.LinkedHashMap
import java.util.Locale
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

    override fun openHttp(endpoint: IrohEndpoint, options: IrohTransportOptions): IrohHttpTransport {
        if (endpoint.scheme() != IrohEndpoint.Scheme.HTTPI) {
            throw IrohTransportException(
                "HTTP transport requires httpi://",
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
                            alpns = listOf(IrohEndpoint.HTTP_ALPN.toByteArray(Charsets.UTF_8)),
                            relayMode = relayMode,
                        ),
                    )
                }
            } catch (error: Throwable) {
                throw structured(error, IrohErrorStage.BIND, IrohDispatchCertainty.NOT_SENT)
            }
            try {
                val remoteId = EndpointId.fromBytes(endpoint.endpointIdBytes())
                val connection = try {
                    withTimeout(remainingMillis()) {
                        local.connect(
                            EndpointAddr(remoteId, options.remoteRelayUrl(), options.directAddresses()),
                            IrohEndpoint.HTTP_ALPN.toByteArray(Charsets.UTF_8),
                        )
                    }
                } catch (error: Throwable) {
                    throw structured(error, IrohErrorStage.CONNECT, IrohDispatchCertainty.NOT_SENT)
                }
                OfficialIrohHttpTransport(local, connection, endpoint.endpointId(), options)
            } catch (error: Throwable) {
                local.shutdown()
                local.close()
                throw error
            }
        }
    }

    override fun supportsHttp(): Boolean = true
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

private class OfficialIrohHttpTransport(
    private val endpoint: Endpoint,
    private val connection: Connection,
    private val remoteId: String,
    private val options: IrohTransportOptions,
) : IrohHttpTransport {
    private val closed = AtomicBoolean()

    override fun execute(request: IrohHttpRequest): IrohHttpResponse {
        if (closed.get()) {
            throw IrohTransportException(
                "Iroh HTTP transport is closed",
                IrohErrorStage.OPEN_STREAM,
                IrohErrorCategory.UNAVAILABLE,
                IrohDispatchCertainty.NOT_SENT,
            )
        }
        val timeout = minOf(request.timeout(), options.ioTimeout())
        val deadline = System.nanoTime() + timeout.toNanos()
        fun remainingMillis(): Long =
            ((deadline - System.nanoTime()) / 1_000_000L).coerceAtLeast(1L)
        return runBlocking {
            val stream = try {
                withTimeout(remainingMillis()) { connection.openBi() }
            } catch (error: Throwable) {
                throw structured(error, IrohErrorStage.OPEN_STREAM, IrohDispatchCertainty.NOT_SENT)
            }
            val send = stream.send()
            val recv = stream.recv()
            try {
                val encoded = encodeHttpRequest(request, remoteId)
                try {
                    withTimeout(remainingMillis()) {
                        send.writeAll(encoded)
                    }
                } catch (error: Throwable) {
                    throw structured(error, IrohErrorStage.WRITE, IrohDispatchCertainty.UNKNOWN)
                }
                val raw = ByteArrayOutputStream()
                val wireLimit = minOf(request.maxResponseBytes(), Int.MAX_VALUE.toLong() - HTTP_HEAD_LIMIT) +
                    HTTP_HEAD_LIMIT
                try {
                    while (true) {
                        val bytes = withTimeout(remainingMillis()) { recv.read(64u shl 10) }
                        if (bytes.isEmpty()) break
                        if (raw.size().toLong() + bytes.size > wireLimit) {
                            throw IrohTransportException(
                                "Iroh HTTP response exceeds configured limit",
                                IrohErrorStage.READ,
                                IrohErrorCategory.RESOURCE_EXHAUSTED,
                                IrohDispatchCertainty.SENT,
                            )
                        }
                        raw.write(bytes)
                    }
                } catch (error: Throwable) {
                    throw structured(error, IrohErrorStage.READ, IrohDispatchCertainty.SENT)
                }
                val response = parseHttpResponse(raw.toByteArray(), request.maxResponseBytes())
                runCatching { withTimeout(remainingMillis()) { send.finish() } }
                response
            } finally {
                runCatching { recv.stop(0uL) }
                send.close()
                recv.close()
                stream.close()
            }
        }
    }

    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        runBlocking {
            runCatching { connection.close(0, byteArrayOf()) }
            runCatching { endpoint.shutdown() }
        }
        connection.close()
        endpoint.close()
    }
}

private const val HTTP_HEAD_LIMIT = 64 * 1024
private val HTTP_TOKEN = Regex("^[!#\$%&'*+.^_`|~0-9A-Za-z-]+\$")

internal fun encodeHttpRequest(request: IrohHttpRequest, remoteId: String): ByteArray {
    val method = request.method()
    val path = request.path()
    if (!HTTP_TOKEN.matches(method) || path.any { it <= ' ' || it.code == 0x7f }) {
        throw IrohTransportException(
            "invalid Iroh HTTP request line",
            IrohErrorStage.WRITE,
            IrohErrorCategory.INVALID_INPUT,
            IrohDispatchCertainty.NOT_SENT,
        )
    }
    val out = ByteArrayOutputStream()
    fun line(value: String) {
        out.write(value.toByteArray(StandardCharsets.ISO_8859_1))
        out.write('\r'.code)
        out.write('\n'.code)
    }
    line("$method $path HTTP/1.1")
    var hasHost = false
    for ((name, values) in request.headers()) {
        if (!HTTP_TOKEN.matches(name)) invalidHttpHeader()
        val lower = name.lowercase(Locale.ROOT)
        if (lower == "content-length" || lower == "transfer-encoding") {
            invalidHttpHeader()
        }
        if (lower == "host") hasHost = true
        for (value in values) {
            if (value.any { it == '\r' || it == '\n' || it.code == 0 || it.code == 0x7f }) {
                invalidHttpHeader()
            }
            line("$name: $value")
        }
    }
    if (!hasHost) line("Host: $remoteId")
    val body = request.body()
    line("Content-Length: ${body.size}")
    line("")
    out.write(body)
    return out.toByteArray()
}

private fun invalidHttpHeader(): Nothing = throw IrohTransportException(
    "invalid or reserved Iroh HTTP request header",
    IrohErrorStage.WRITE,
    IrohErrorCategory.INVALID_INPUT,
    IrohDispatchCertainty.NOT_SENT,
)

internal fun parseHttpResponse(wire: ByteArray, bodyLimit: Long): IrohHttpResponse {
    val headEnd = findHeaderEnd(wire)
    if (headEnd < 0 || headEnd > HTTP_HEAD_LIMIT) {
        protocolFailure("invalid or oversized HTTP response head (${wire.size} bytes)")
    }
    val lines = String(wire, 0, headEnd, StandardCharsets.ISO_8859_1).split("\r\n")
    val statusParts = lines.firstOrNull()?.split(' ', limit = 3) ?: protocolFailure("missing HTTP status")
    if (statusParts.size < 2 || statusParts[0] !in setOf("HTTP/1.0", "HTTP/1.1")) {
        protocolFailure("invalid HTTP status line: ${lines.firstOrNull()?.take(80)}")
    }
    val status = statusParts[1].toIntOrNull() ?: protocolFailure("invalid HTTP status")
    if (status !in 100..999) protocolFailure("invalid HTTP status")
    val headers = LinkedHashMap<String, MutableList<String>>()
    for (line in lines.drop(1)) {
        val colon = line.indexOf(':')
        if (colon <= 0) protocolFailure("invalid HTTP response header")
        val name = line.substring(0, colon).lowercase(Locale.ROOT)
        val value = line.substring(colon + 1).trim(' ', '\t')
        if (!HTTP_TOKEN.matches(name) || value.any { it == '\r' || it == '\n' || it.code == 0 || it.code == 0x7f }) {
            protocolFailure("invalid HTTP response header")
        }
        headers.getOrPut(name) { mutableListOf() }.add(value)
    }
    val bodyStart = headEnd + 4
    val transfer = headers["transfer-encoding"].orEmpty()
    val lengths = headers["content-length"].orEmpty()
    if (transfer.isNotEmpty() && lengths.isNotEmpty()) protocolFailure("conflicting HTTP response framing")
    val body = when {
        transfer.isNotEmpty() -> {
            if (transfer.size != 1 || transfer.single().lowercase(Locale.ROOT) != "chunked") {
                protocolFailure("unsupported HTTP transfer encoding")
            }
            decodeChunked(wire, bodyStart, bodyLimit)
        }
        lengths.isNotEmpty() -> {
            if (lengths.size != 1 || !lengths.single().matches(Regex("^(0|[1-9][0-9]*)\$"))) {
                protocolFailure("invalid HTTP content length")
            }
            val length = lengths.single().toLongOrNull() ?: protocolFailure("invalid HTTP content length")
            if (length > bodyLimit || length > Int.MAX_VALUE) responseTooLarge()
            if (wire.size - bodyStart != length.toInt()) protocolFailure("truncated or excess HTTP response body")
            wire.copyOfRange(bodyStart, wire.size)
        }
        else -> {
            val length = wire.size - bodyStart
            if (length.toLong() > bodyLimit) responseTooLarge()
            wire.copyOfRange(bodyStart, wire.size)
        }
    }
    return IrohHttpResponse(status, headers, body)
}

private fun findHeaderEnd(bytes: ByteArray): Int {
    val limit = minOf(bytes.size - 3, HTTP_HEAD_LIMIT + 1)
    for (index in 0 until maxOf(0, limit)) {
        if (bytes[index] == '\r'.code.toByte() && bytes[index + 1] == '\n'.code.toByte() &&
            bytes[index + 2] == '\r'.code.toByte() && bytes[index + 3] == '\n'.code.toByte()
        ) return index
    }
    return -1
}

private fun decodeChunked(wire: ByteArray, start: Int, bodyLimit: Long): ByteArray {
    var cursor = start
    val body = ByteArrayOutputStream()
    while (true) {
        val lineEnd = findCrlf(wire, cursor)
        if (lineEnd < 0 || lineEnd - cursor > HTTP_HEAD_LIMIT) protocolFailure("invalid HTTP chunk header")
        val rawSize = String(wire, cursor, lineEnd - cursor, StandardCharsets.US_ASCII)
            .substringBefore(';').trim()
        val size = rawSize.toLongOrNull(16) ?: protocolFailure("invalid HTTP chunk size")
        cursor = lineEnd + 2
        if (size == 0L) {
            while (true) {
                val trailerEnd = findCrlf(wire, cursor)
                if (trailerEnd < 0) protocolFailure("truncated HTTP chunk trailer")
                if (trailerEnd == cursor) {
                    cursor += 2
                    if (cursor != wire.size) protocolFailure("excess bytes after HTTP chunked body")
                    return body.toByteArray()
                }
                cursor = trailerEnd + 2
            }
        }
        if (size > bodyLimit - body.size() || size > Int.MAX_VALUE) responseTooLarge()
        val end = cursor.toLong() + size
        if (end + 2 > wire.size || wire[end.toInt()] != '\r'.code.toByte() ||
            wire[end.toInt() + 1] != '\n'.code.toByte()
        ) protocolFailure("truncated HTTP chunk")
        body.write(wire, cursor, size.toInt())
        cursor = end.toInt() + 2
    }
}

private fun findCrlf(bytes: ByteArray, start: Int): Int {
    for (index in start until bytes.size - 1) {
        if (bytes[index] == '\r'.code.toByte() && bytes[index + 1] == '\n'.code.toByte()) return index
    }
    return -1
}

private fun protocolFailure(message: String): Nothing = throw IrohTransportException(
    message,
    IrohErrorStage.READ,
    IrohErrorCategory.PROTOCOL,
    IrohDispatchCertainty.SENT,
)

private fun responseTooLarge(): Nothing = throw IrohTransportException(
    "Iroh HTTP response exceeds configured limit",
    IrohErrorStage.READ,
    IrohErrorCategory.RESOURCE_EXHAUSTED,
    IrohDispatchCertainty.SENT,
)
