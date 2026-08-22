package com.margelo.nitro.nitrosseclient

import android.util.Log
import okhttp3.Call
import okhttp3.Callback
import okhttp3.Connection
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
// only sees the OkHttp Call) can be correlated back to the same connection attempt when it closes.
private data class ConnectionAttempt(val generation: Int)

class HybridSSEClient : HybridSSEClientSpec() {
  override var onMessage: (event: SSEMessageEvent) -> Unit = {}
  override var onOpen: () -> Unit = {}
  override var onError: (message: String) -> Unit = {}
  override var onMetrics: (metrics: SSEConnectionMetrics) -> Unit = {}

  private val timingsByGeneration = mutableMapOf<Int, CallTimings>()
  private var generationCounter = 0

  // Created once and reused for every connect()/disconnect() cycle so its connection pool
  // survives reconnects — reconnects reuse pooled HTTP/2 connections instead of re-handshaking.
  //
  // We talk to OkHttp directly (client.newCall(...).enqueue(...)) rather than through
  // okhttp-sse's EventSource: RealEventSource.connect() internally does
  // `client.newBuilder().eventListener(...).build()`, which silently replaces this
  // eventListenerFactory, so our handshake/TLS timings would never fire if routed through it.
  private val client: OkHttpClient = OkHttpClient.Builder()
    .readTimeout(0, TimeUnit.MILLISECONDS)
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

  private var currentCall: Call? = null
  private var connectStartedAt: Long? = null
  private var firstByteLogged = false

  override fun connect(url: String) {
    currentCall?.let { emitCloseMetrics(it) }
    currentCall?.cancel()

    firstByteLogged = false
    connectStartedAt = System.nanoTime()
    generationCounter += 1
    val generation = generationCounter

    val request = Request.Builder()
      .url(url)
      .header("Accept", "text/event-stream")
      // Some SSE providers (e.g. Wikimedia) reject requests carrying OkHttp's generic default
      // User-Agent with 403; identify this library instead of leaving it unset.
      .header("User-Agent", "react-native-nitro-sse-client (+https://github.com/TaranVitalii/react-native-nitro-sse-client)")
      .tag(ConnectionAttempt::class.java, ConnectionAttempt(generation))
      .build()

    val call = client.newCall(request)
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

  private fun maybeLogFirstByte() {
    if (firstByteLogged) return
    firstByteLogged = true
    val startedAt = connectStartedAt ?: return
    val ttfbMs = (System.nanoTime() - startedAt) / 1_000_000.0
    Log.d(LOG_TAG, "time to first data: ${"%.1f".format(ttfbMs)}ms")
    onMetrics(
      SSEConnectionMetrics(
        phase = SSEMetricsPhase.TTFB,
        ttfbMs = ttfbMs,
        dnsMs = null,
        connectMs = null,
        tlsMs = null,
        connectionReused = null,
        timestampMs = System.currentTimeMillis().toDouble()
      )
    )
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
    val timings = timingsByGeneration.remove(attempt.generation) ?: return

    fun durationMs(start: Long?, end: Long?): Double? {
      if (start == null || end == null) return null
      return (end - start) / 1_000_000.0
    }

    val connectMs = durationMs(timings.connectStart, timings.connectEnd)
    val tlsMs = durationMs(timings.secureConnectStart, timings.secureConnectEnd)
    val ttfbMs = durationMs(timings.callStart, timings.responseHeadersStart)

    Log.d(
      LOG_TAG,
      "connection closed reused=${timings.connectionReused} connect=${connectMs}ms tls=${tlsMs}ms ttfb=${ttfbMs}ms"
    )

    onMetrics(
      SSEConnectionMetrics(
        phase = SSEMetricsPhase.CLOSED,
        ttfbMs = ttfbMs,
        dnsMs = null,
        connectMs = connectMs,
        tlsMs = tlsMs,
        connectionReused = timings.connectionReused,
        timestampMs = System.currentTimeMillis().toDouble()
      )
    )
  }
}
