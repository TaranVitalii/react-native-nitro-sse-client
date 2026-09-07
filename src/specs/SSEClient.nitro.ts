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
 * react-native-sse model: reconnect is on by default, at a flat interval the server can override
 * per-stream via an SSE `retry:` field (no exponential backoff).
 */
export interface SSEReconnectOptions {
  /** Default true. */
  enabled?: boolean
  /**
   * Delay before the first/next reconnect attempt, in ms. Default 3000. A `retry:` field in the
   * stream overrides this for that stream's subsequent reconnects (until connect() is called
   * again explicitly, which resets it back to this value).
   */
  intervalMs?: number
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

export interface SSEClient
  extends HybridObject<{ ios: 'swift'; android: 'kotlin' }> {
  connect(
    url: string,
    headers?: Record<string, string>,
    session?: SSESessionOptions,
    reconnect?: SSEReconnectOptions,
    method?: string,
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
}
