package com.margelo.nitro.nitrosseclient

import android.util.Log
import okhttp3.Call
import okhttp3.Callback
import okhttp3.Connection
import okhttp3.Dispatcher
import okhttp3.EventListener
import okhttp3.Handshake
import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.Request
import okhttp3.Response
import java.io.IOException
import java.net.InetSocketAddress
import java.net.Proxy
import java.util.concurrent.TimeUnit

private const val LOG_TAG = "NativeSSE"

private class CallTimings {
  var connectStart: Long? = null
  var connectEnd: Long? = null
  var secureConnectStart: Long? = null
  var secureConnectEnd: Long? = null
  var callStart: Long? = null
  var responseHeadersStart: Long? = null
  var connectionReused = false
}

// Tags the Request for a given connect() attempt so the shared client's EventListener (which
// only sees the OkHttp Call) can be correlated back to the same connection attempt when it
// closes. generation is unique across every HybridSSEClient instance, purely as a correlation key.
private data class ConnectionAttempt(val generation: Int)

/**
 * Shared by every HybridSSEClient instance — created once and reused for every connect()/
 * disconnect() cycle across ALL instances (not just reconnects on the same one), so the
 * connection pool persists no matter which instance is talking to a given host.
 */
private object SharedClient {
  private var client: OkHttpClient? = null
  val timingsByGeneration = mutableMapOf<Int, CallTimings>()
  private var generationCounter = 0

  @Synchronized
  fun nextGeneration(): Int {
    generationCounter += 1
    return generationCounter
  }

  // Lazily created from whichever connect() call comes first, across every instance — later
  // calls' `session` options are silently ignored once this exists, since recreating it would
  // drop every connection already in the pool. Synchronized so two instances racing to connect()
  // for the first time can't each create their own client.
  @Synchronized
  fun get(options: SSESessionOptions?): OkHttpClient {
    client?.let { return it }

    val readTimeoutSeconds = options?.timeoutSeconds ?: 0.0 // matches the previous default: no timeout
    // OkHttp has no direct equivalent of iOS's httpMaximumConnectionsPerHost; maxRequestsPerHost
    // is the closest analogue (concurrent requests to a single host), defaulting to OkHttp's own
    // built-in default when not specified.
    val maxRequestsPerHost = options?.maxConnectionsPerHost?.toInt() ?: Dispatcher().maxRequestsPerHost

    // We talk to OkHttp directly (client.newCall(...).enqueue(...)) rather than through
    // okhttp-sse's EventSource: RealEventSource.connect() internally does
    // `client.newBuilder().eventListener(...).build()`, which silently replaces this
    // eventListenerFactory, so our handshake/TLS timings would never fire if routed through it.
    val newClient = OkHttpClient.Builder()
      .readTimeout(readTimeoutSeconds.toLong(), TimeUnit.SECONDS)
      .dispatcher(Dispatcher().apply { this.maxRequestsPerHost = maxRequestsPerHost })
      .eventListenerFactory { call ->
        val attempt = call.request().tag(ConnectionAttempt::class.java)
        val timings = CallTimings()
        if (attempt != null) timingsByGeneration[attempt.generation] = timings

        object : EventListener() {
          override fun callStart(call: Call) {
            timings.callStart = System.nanoTime()
          }

          override fun connectStart(call: Call, inetSocketAddress: InetSocketAddress, proxy: Proxy) {
            timings.connectStart = System.nanoTime()
          }

          override fun secureConnectStart(call: Call) {
            timings.secureConnectStart = System.nanoTime()
          }

          override fun secureConnectEnd(call: Call, handshake: Handshake?) {
            timings.secureConnectEnd = System.nanoTime()
          }

          override fun connectEnd(call: Call, inetSocketAddress: InetSocketAddress, proxy: Proxy, protocol: Protocol?) {
            timings.connectEnd = System.nanoTime()
          }

          override fun connectionAcquired(call: Call, connection: Connection) {
            // If connectStart never fired for this call, OkHttp handed us an already-pooled
            // connection instead of opening a new one — that absence is the reuse signal.
            timings.connectionReused = timings.connectStart == null
          }

          override fun responseHeadersStart(call: Call) {
            timings.responseHeadersStart = System.nanoTime()
          }
        }
      }
      .build()

    client = newClient
    return newClient
  }
}

class HybridSSEClient : HybridSSEClientSpec() {
  override var onMessage: (event: SSEMessageEvent) -> Unit = {}
  override var onOpen: () -> Unit = {}
  override var onError: (message: String) -> Unit = {}
  override var onMetrics: (metrics: SSEConnectionMetrics) -> Unit = {}

  private var currentCall: Call? = null
  private var connectStartedAt: Long? = null
  private var firstByteLogged = false

  override fun connect(url: String, session: SSESessionOptions?) {
    currentCall?.let { emitCloseMetrics(it) }
    currentCall?.cancel()

    firstByteLogged = false
    connectStartedAt = System.nanoTime()
    val generation = SharedClient.nextGeneration()

    val request = Request.Builder()
      .url(url)
      .header("Accept", "text/event-stream")
      // Some SSE providers (e.g. Wikimedia) reject requests carrying OkHttp's generic default
      // User-Agent with 403; identify this library instead of leaving it unset.
      .header("User-Agent", "react-native-nitro-sse-client (+https://github.com/TaranVitalii/react-native-nitro-sse-client)")
      .tag(ConnectionAttempt::class.java, ConnectionAttempt(generation))
      .build()

    val call = SharedClient.get(session).newCall(request)
    currentCall = call

    call.enqueue(object : Callback {
      override fun onResponse(call: Call, response: Response) {
        onOpen()
        try {
          val source = response.body?.source()
          if (source == null) {
            response.close()
            return
          }
          val eventBuffer = StringBuilder()
          var loggedFirstByte = false
          while (!source.exhausted()) {
            if (!loggedFirstByte) {
              maybeLogFirstByte()
              loggedFirstByte = true
            }
            val line = source.readUtf8Line() ?: break
            if (line.isEmpty()) {
              if (eventBuffer.isNotEmpty()) {
                parseAndEmit(eventBuffer.toString())
                eventBuffer.setLength(0)
              }
            } else {
              eventBuffer.append(line).append('\n')
            }
          }
        } catch (e: IOException) {
          // Stream ended, or was cancelled by disconnect()/a superseding connect() — expected.
        } finally {
          emitCloseMetrics(call)
          response.close()
        }
      }

      override fun onFailure(call: Call, e: IOException) {
        emitCloseMetrics(call)
        if (call.isCanceled()) return
        Log.e(LOG_TAG, "connect failed: ${e.message}", e)
        onError(e.message ?: e.toString())
      }
    })
  }

  override fun disconnect() {
    currentCall?.let { emitCloseMetrics(it) }
    currentCall?.cancel()
    currentCall = null
  }

  // Native-only diagnostic — not sent to JS. onMetrics is limited to connectionReused (see
  // SSEConnectionMetrics on the JS side), which isn't knowable until the connection closes.
  private fun maybeLogFirstByte() {
    if (firstByteLogged) return
    firstByteLogged = true
    val startedAt = connectStartedAt ?: return
    val ttfbMs = (System.nanoTime() - startedAt) / 1_000_000.0
    Log.d(LOG_TAG, "time to first data: ${"%.1f".format(ttfbMs)}ms")
  }

  private fun parseAndEmit(rawEvent: String) {
    var id: String? = null
    var eventName: String? = null
    val dataLines = mutableListOf<String>()

    for (line in rawEvent.split("\n")) {
      when {
        line.startsWith(":") -> Unit
        line.startsWith("id:") -> id = line.removePrefix("id:").trim()
        line.startsWith("event:") -> eventName = line.removePrefix("event:").trim()
        line.startsWith("data:") -> dataLines.add(line.removePrefix("data:").trim())
      }
    }

    if (dataLines.isEmpty()) return

    onMessage(
      SSEMessageEvent(id, eventName, dataLines.joinToString("\n"), System.currentTimeMillis().toDouble())
    )
  }

  private fun emitCloseMetrics(call: Call) {
    val attempt = call.request().tag(ConnectionAttempt::class.java) ?: return
    val timings = SharedClient.timingsByGeneration.remove(attempt.generation) ?: return

    fun durationMs(start: Long?, end: Long?): Double? {
      if (start == null || end == null) return null
      return (end - start) / 1_000_000.0
    }

    val connectMs = durationMs(timings.connectStart, timings.connectEnd)
    val tlsMs = durationMs(timings.secureConnectStart, timings.secureConnectEnd)
    val ttfbMs = durationMs(timings.callStart, timings.responseHeadersStart)

    // Full breakdown stays native-only (log line) — only connectionReused crosses the bridge.
    Log.d(
      LOG_TAG,
      "connection closed reused=${timings.connectionReused} connect=${connectMs}ms tls=${tlsMs}ms ttfb=${ttfbMs}ms"
    )

    onMetrics(SSEConnectionMetrics(connectionReused = timings.connectionReused))
  }
}
