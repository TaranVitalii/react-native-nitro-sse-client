import type { HybridObject } from 'react-native-nitro-modules'

export interface SSEMessageEvent {
  id?: string
  event?: string
  data: string
  timestampMs: number
}

/**
 * Fires once, when a connection ends (connect() was superseded, disconnect() was called, or it
 * failed) — connectionReused is only knowable at that point: on iOS it comes from
 * URLSessionTaskMetrics, which the OS only hands over once the task has fully finished, so
 * there's no way to report this any earlier (e.g. alongside onOpen) on either platform.
 */
export interface SSEConnectionMetrics {
  connectionReused: boolean
}

/**
 * The underlying URLSession (iOS) / OkHttpClient (Android) is shared by every SSEClient
 * instance and, once created, kept alive for the app's lifetime — that's what lets a reconnect
 * reuse the pooled HTTP/2 connection instead of re-handshaking. Because of that, this only takes
 * effect once, on whichever connect() call ends up being the very first one made across ALL
 * instances in the app; after that, the shared client already exists and this is ignored.
 * Prefer configureSSESession() (in the JS wrapper) over passing this directly — it removes the
 * guesswork of "which instance connects first" by setting this once, up front, at app startup.
 */
export interface SSESessionOptions {
  /** Request timeout in seconds. Defaults to 3600 on iOS, unlimited (0) on Android. */
  timeoutSeconds?: number
  /**
   * Max concurrent connections to a single host. Defaults to 6 on iOS. On Android this maps to
   * OkHttp's Dispatcher.maxRequestsPerHost (its closest equivalent — HTTP/2 hosts multiplex many
   * requests over one connection regardless), defaulting to OkHttp's own default (5).
   */
  maxConnectionsPerHost?: number
}

/**
 * 'http': the server responded, but with a non-2xx status — `statusCode` and `message` (the
 * response body, if any) are populated.
 * 'invalid-content-type': the server responded 2xx, but with a Content-Type other than
 * `text/event-stream` — usually a misconfigured server/proxy (a login redirect page, a JSON
 * error body dressed up as 200, etc). `message` describes what was received.
 * 'timeout': the request's own timeout (SSESessionOptions.timeoutSeconds) elapsed with no
 * response.
 * 'network': a transport-level failure (DNS, connection refused, TLS, dropped connection, etc.)
 * — `message` is the OS's own error description.
 * 'exception': the call couldn't even be attempted (e.g. an invalid URL).
 */
export type SSEErrorType =
  | 'http'
  | 'invalid-content-type'
  | 'network'
  | 'timeout'
  | 'exception'

export interface SSEError {
  message: string
  type: SSEErrorType
  /** Only set when type is 'http'. */
  statusCode?: number
}

/**
 * Governs automatic reconnection after a connection ends for any reason (error, or the server
 * closing the stream) other than an explicit disconnect(). Mirrors the browser EventSource /
 * react-native-sse model, with exponential backoff + jitter layered on top (see intervalMs,
 * maxIntervalMs, jitterFactor) rather than react-native-sse's flat delay.
 */
export interface SSEReconnectOptions {
  /** Default true. */
  enabled?: boolean
  /**
   * Base delay before the first reconnect attempt, in ms. Default 3000. A `retry:` field in the
   * stream overrides this for that stream's subsequent reconnects (until connect() is called
   * again explicitly, which resets it back to this value). Each consecutive failed attempt after
   * the first doubles the delay (see maxIntervalMs, jitterFactor) — this is the starting point,
   * not a flat per-attempt delay.
   */
  intervalMs?: number
  /**
   * Cap on the exponential backoff delay, in ms. Default 30000. Once doubling from intervalMs
   * would exceed this, the delay stays at this value for every subsequent attempt.
   */
  maxIntervalMs?: number
  /**
   * Randomizes each computed backoff delay by this fraction (0.0-1.0), so e.g. a jitterFactor of
   * 0.5 turns a computed 4000ms delay into a random value in [3000, 5000]. Default 0.5 — this
   * spreads out reconnect attempts from many clients hitting the same outage at once ("thundering
   * herd"), so they don't all retry in lockstep. 0 disables jitter (exact exponential delay).
   */
  jitterFactor?: number
  /**
   * Stop reconnecting after this many consecutive failed attempts. Default undefined (retry
   * forever). Resets to 0 after any successful onOpen.
   */
  maxAttempts?: number
  /**
   * By default, a 4xx response (client error) or a Content-Type mismatch does NOT trigger a
   * reconnect — that class of failure reflects something wrong with the request/server config
   * that retrying identically won't fix. The one exception retried by default regardless is 429
   * (rate limited — a deliberate "back off and try again" signal). 5xx, network, and timeout
   * errors always retry (subject to maxAttempts). Set true to retry every HTTP error, including
   * 4xx.
   */
  retryOnClientError?: boolean
}

/**
 * 'idle': never connected, or destroy()ed — the initial state.
 * 'connecting': an explicit connect() call's first attempt is in flight, before its first
 * onOpen/onError.
 * 'open': the connection is live, after onOpen.
 * 'reconnecting': an automatic retry is pending (waiting out the backoff delay) or in flight,
 * after the connection ended for a reason SSEReconnectOptions allows retrying.
 * 'closed': ended intentionally — an explicit disconnect(), or the connection ended while
 * reconnect.enabled was false.
 * 'failed': automatic reconnect gave up on this connect() session — either a non-retryable error
 * (see SSEReconnectOptions.retryOnClientError) or reconnect.maxAttempts was reached. A fresh
 * connect() call is needed to try again.
 */
export type SSEConnectionState =
  | 'idle'
  | 'connecting'
  | 'open'
  | 'reconnecting'
  | 'closed'
  | 'failed'

export interface SSEClient
  extends HybridObject<{ ios: 'swift'; android: 'kotlin' }> {
  connect(
    url: string,
    headers?: Record<string, string>,
    session?: SSESessionOptions,
    reconnect?: SSEReconnectOptions,
    // Named httpMethod, not method: nitrogen's own generated Android/JNI binding code declares a
    // local variable literally named `method` inside every wrapped function (the looked-up Java
    // Method object) — a parameter also named `method` collides with it, causing a C++
    // "redefinition of 'method'" compile error that only surfaces on Android (Swift's codegen
    // doesn't hit this). Purely an internal rename — the JS-facing SSEConnectOptions.method field
    // name is unaffected.
    httpMethod?: string,
    body?: string,
    /** Default true. A non-`text/event-stream` Content-Type on an otherwise-successful response
     * is reported via onError (type 'invalid-content-type') instead of being treated as open.
     * Set false for a server that's valid SSE but sends a different/no Content-Type. */
    validateContentType?: boolean
  ): void
  disconnect(): void
  onMessage: (event: SSEMessageEvent) => void
  onOpen: () => void
  onError: (error: SSEError) => void
  /** Fires whenever the connection ends, for any reason — including a normal server-side close,
   * an error (after onError), or an explicit disconnect(). Does not fire again for connections
   * superseded by a newer connect() before they ever opened. */
  onClose: () => void
  onMetrics: (metrics: SSEConnectionMetrics) => void
  /** Fires on every state transition — see SSEConnectionState. Only fires when the state actually
   * changes (no duplicate events for the same state). */
  onStateChange: (state: SSEConnectionState) => void
  /**
   * Awaited immediately before every request this stream makes — the initial connect() and every
   * automatic reconnect alike — so it's the right place to refresh a short-lived auth token
   * rather than letting a reconnect fire with a stale one. Whatever headers it resolves with are
   * merged over the connect()-time headers (resolved values win on a key collision). Defaults to
   * a no-op that resolves immediately with no extra headers.
   */
  onBeforeRequest: () => Promise<Record<string, string>>
}
