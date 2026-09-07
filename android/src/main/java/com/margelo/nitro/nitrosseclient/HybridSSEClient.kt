package com.margelo.nitro.nitrosseclient

import android.os.Handler
import android.os.Looper
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
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import okio.Buffer
import java.io.IOException
import java.net.InetSocketAddress
import java.net.Proxy
import java.net.SocketTimeoutException
import java.util.concurrent.TimeUnit

private const val LOG_TAG = "NativeSSE"
private const val DEFAULT_RECONNECT_INTERVAL_MS = 3000.0

// Error responses (4xx/5xx) are typically small JSON/HTML bodies — bounded so a misbehaving
// server streaming an enormous error page can't grow this unboundedly before completion.
private const val MAX_ERROR_BODY_BYTES = 8192L

// OkHttp throws if these methods are given a null body — an empty one is substituted when the
// caller specifies e.g. method: 'POST' without a body.
private val METHODS_REQUIRING_BODY = setOf("POST", "PUT", "PATCH", "PROPPATCH", "REPORT")

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
  override var onError: (error: SSEError) -> Unit = {}
  override var onClose: () -> Unit = {}
  override var onMetrics: (metrics: SSEConnectionMetrics) -> Unit = {}

  private var currentCall: Call? = null
  private var connectStartedAt: Long? = null
  private var firstByteLogged = false

  // Reconnect state. connectUrl/connectHeaders/connectMethod/connectBody/connectSession are
  // remembered so an automatic reconnect can repeat the same connect() call;
  // reconnectEnabled/reconnectMaxAttempts/retryOnClientError come from the caller's
  // SSEReconnectOptions, resolved once per explicit connect(). currentIntervalMs starts at
  // options.intervalMs (default 3s) and is overridden per-stream by a `retry:` field from the
  // server; it resets back to the option's value on the next *explicit* connect().
  private var connectUrl: String? = null
  private var connectHeaders: Map<String, String>? = null
  private var connectMethod: String? = null
  private var connectBody: String? = null
  private var connectSession: SSESessionOptions? = null
  private var shouldValidateContentType = true
  private var reconnectEnabled = true
  private var reconnectMaxAttempts: Double? = null
  private var retryOnClientError = false
  private var currentIntervalMs = DEFAULT_RECONNECT_INTERVAL_MS
  private var reconnectAttempts = 0
  private val mainHandler = Handler(Looper.getMainLooper())
  private var pendingReconnect: Runnable? = null
  // Set by disconnect(), cleared at the start of every explicit connect() — distinguishes "the
  // caller asked us to stop" from a connection merely ending, which is what schedules a retry.
  private var intentionallyStopped = false
  private var lastEventId: String? = null

  override fun connect(
    url: String,
    headers: Map<String, String>?,
    session: SSESessionOptions?,
    reconnect: SSEReconnectOptions?,
    method: String?,
    body: String?,
    validateContentType: Boolean?
  ) {
    // Validated before touching any existing connection, so a bad URL on a reconnect attempt
    // doesn't tear down a connection that was working fine — and reported through onError like
    // any other connection failure, rather than left to crash as an uncaught IllegalArgumentException.
    try {
      Request.Builder().url(url)
    } catch (e: IllegalArgumentException) {
      Log.e(LOG_TAG, "invalid URL: $url", e)
      onError(SSEError("Invalid URL: $url", SSEErrorType.EXCEPTION, null))
      return
    }

    pendingReconnect?.let { mainHandler.removeCallbacks(it) }
    pendingReconnect = null
    intentionallyStopped = false
    reconnectAttempts = 0
    lastEventId = null

    connectUrl = url
    connectHeaders = headers
    connectMethod = method
    connectBody = body
    connectSession = session
    shouldValidateContentType = validateContentType ?: true
    reconnectEnabled = reconnect?.enabled ?: true
    currentIntervalMs = reconnect?.intervalMs ?: DEFAULT_RECONNECT_INTERVAL_MS
    reconnectMaxAttempts = reconnect?.maxAttempts
    retryOnClientError = reconnect?.retryOnClientError ?: false

    performConnect(isReconnect = false)
  }

  private fun performConnect(isReconnect: Boolean) {
    val url = connectUrl ?: return
    val requestBuilder = Request.Builder().url(url)
    applyMethodAndBody(requestBuilder, connectMethod, connectBody)

    currentCall?.let { emitCloseMetrics(it) }
    currentCall?.cancel()

    firstByteLogged = false
    connectStartedAt = System.nanoTime()
    val generation = SharedClient.nextGeneration()

    requestBuilder
      .header("Accept", "text/event-stream")
      // Some SSE providers (e.g. Wikimedia) reject requests carrying OkHttp's generic default
      // User-Agent with 403; identify this library instead of leaving it unset.
      .header("User-Agent", "react-native-nitro-sse-client (+https://github.com/TaranVitalii/react-native-nitro-sse-client)")
      .tag(ConnectionAttempt::class.java, ConnectionAttempt(generation))

    // Applied after the defaults above, so a caller can override Accept/User-Agent too if they
    // need to — e.g. Authorization for a protected endpoint.
    connectHeaders?.forEach { (key, value) -> requestBuilder.header(key, value) }

    // Only sent on an automatic reconnect that has actually seen an id: field — an explicit
    // connect() always starts a fresh logical session (see lastEventId reset in connect()).
    if (isReconnect) {
      lastEventId?.let { requestBuilder.header("Last-Event-ID", it) }
    }

    val request = requestBuilder.build()
    val call = SharedClient.get(connectSession).newCall(request)
    currentCall = call

    call.enqueue(object : Callback {
      override fun onResponse(call: Call, response: Response) {
        // A superseding connect() may have already cancelled this call and started a new one
        // before this callback for the OLD call's response arrives — without this check, a late
        // response like this would fire onOpen()/onMessage() for a connection that's no longer
        // the active one.
        if (call !== currentCall) {
          response.close()
          return
        }

        if (!response.isSuccessful) {
          val statusCode = response.code
          val bodyString = readBoundedBody(response)
          response.close()
          Log.e(LOG_TAG, "HTTP error: $statusCode")
          onError(SSEError(bodyString, SSEErrorType.HTTP, statusCode.toDouble()))
          onClose()
          scheduleReconnectIfNeeded(httpStatus = statusCode, wasContentTypeError = false)
          return
        }

        if (shouldValidateContentType) {
          val contentType = response.header("Content-Type")?.lowercase() ?: ""
          if (!contentType.startsWith("text/event-stream")) {
            val bodyString = readBoundedBody(response)
            response.close()
            Log.e(LOG_TAG, "unexpected Content-Type: $contentType")
            val message = bodyString.ifEmpty { "Response Content-Type was not text/event-stream" }
            onError(SSEError(message, SSEErrorType.INVALID_CONTENT_TYPE, null))
            onClose()
            scheduleReconnectIfNeeded(httpStatus = null, wasContentTypeError = true)
            return
          }
        }

        reconnectAttempts = 0
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
            if (call !== currentCall) break
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
          // Cancelled by disconnect()/a superseding connect() — expected, already handled
          // elsewhere (disconnect() fires onClose synchronously; a superseding call's own
          // lifecycle governs). Anything else reaching here is a genuine mid-stream failure.
          if (!call.isCanceled() && call === currentCall) {
            Log.e(LOG_TAG, "connection dropped: ${e.message}", e)
            val type = if (e is SocketTimeoutException) SSEErrorType.TIMEOUT else SSEErrorType.NETWORK
            onError(SSEError(e.message ?: e.toString(), type, null))
          }
        } finally {
          emitCloseMetrics(call)
          response.close()
        }

        if (call === currentCall) {
          onClose()
          scheduleReconnectIfNeeded(httpStatus = null, wasContentTypeError = false)
        }
      }

      override fun onFailure(call: Call, e: IOException) {
        emitCloseMetrics(call)
        if (call.isCanceled()) return
        // A genuine (non-cancellation) failure on a call a newer connect() has already
        // superseded shouldn't surface as "the current connection failed" — it isn't, anymore.
        if (call !== currentCall) return
        Log.e(LOG_TAG, "connect failed: ${e.message}", e)
        val type = if (e is SocketTimeoutException) SSEErrorType.TIMEOUT else SSEErrorType.NETWORK
        onError(SSEError(e.message ?: e.toString(), type, null))
        onClose()
        scheduleReconnectIfNeeded(httpStatus = null, wasContentTypeError = false)
      }
    })
  }

  // GET/HEAD must carry a null body; POST/PUT/PATCH etc. throw from OkHttp if given a null one —
  // an empty body is substituted so `method: 'POST'` alone (no body) doesn't crash.
  private fun applyMethodAndBody(builder: Request.Builder, method: String?, body: String?) {
    val resolvedMethod = method?.uppercase() ?: "GET"
    if (resolvedMethod !in METHODS_REQUIRING_BODY && body == null) {
      builder.method(resolvedMethod, null)
      return
    }
    builder.method(resolvedMethod, (body ?: "").toRequestBody())
  }

  private fun readBoundedBody(response: Response): String {
    val source = response.body?.source() ?: return ""
    return try {
      val buffer = Buffer()
      while (buffer.size < MAX_ERROR_BODY_BYTES && !source.exhausted()) {
        val read = source.read(buffer, MAX_ERROR_BODY_BYTES - buffer.size)
        if (read == -1L) break
      }
      buffer.readUtf8()
    } catch (e: IOException) {
      ""
    }
  }

  // A 4xx status (other than 429, a rate-limit signal worth retrying) reflects something wrong
  // with the request/server config that retrying identically won't fix — same for a Content-Type
  // mismatch. Both are skipped by default; retryOnClientError opts back into the old
  // retry-everything behavior.
  private fun isRetryableByDefault(httpStatus: Int?, wasContentTypeError: Boolean): Boolean {
    if (retryOnClientError) return true
    if (wasContentTypeError) return false
    if (httpStatus == null) return true
    if (httpStatus == 429) return true
    return httpStatus !in 400..499
  }

  private fun scheduleReconnectIfNeeded(httpStatus: Int?, wasContentTypeError: Boolean) {
    if (!reconnectEnabled || intentionallyStopped) return
    if (!isRetryableByDefault(httpStatus, wasContentTypeError)) return
    reconnectMaxAttempts?.let { max -> if (reconnectAttempts >= max) return }
    reconnectAttempts += 1

    val runnable = Runnable {
      if (!intentionallyStopped) performConnect(isReconnect = true)
    }
    pendingReconnect = runnable
    mainHandler.postDelayed(runnable, currentIntervalMs.toLong())
  }

  override fun disconnect() {
    pendingReconnect?.let { mainHandler.removeCallbacks(it) }
    pendingReconnect = null
    intentionallyStopped = true
    currentCall?.let { emitCloseMetrics(it) }
    val hadActiveCall = currentCall != null
    currentCall?.cancel()
    currentCall = null
    if (hadActiveCall) onClose()
  }

  // Called by Nitro when the JS side releases this object (GC), or explicitly via
  // nativeObject.dispose() — without this, an abandoned SSEStream that never called
  // disconnect()/destroy() would leave its call running against the shared client forever.
  override fun dispose() {
    super.dispose()
    pendingReconnect?.let { mainHandler.removeCallbacks(it) }
    currentCall?.cancel()
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
        line.startsWith("retry:") -> {
          line.removePrefix("retry:").trim().toDoubleOrNull()?.let { currentIntervalMs = it }
        }
      }
    }

    // An `id:` field (even empty) updates lastEventId for the *next* reconnect's Last-Event-ID
    // header — empty resets it to unset, matching the SSE spec. Absent leaves it unchanged.
    if (id != null) {
      lastEventId = id.ifEmpty { null }
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
