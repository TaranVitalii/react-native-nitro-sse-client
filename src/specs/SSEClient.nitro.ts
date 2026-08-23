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

export interface SSEClient
  extends HybridObject<{ ios: 'swift'; android: 'kotlin' }> {
  connect(url: string, session?: SSESessionOptions): void
  disconnect(): void
  onMessage: (event: SSEMessageEvent) => void
  onOpen: () => void
  onError: (message: string) => void
  onMetrics: (metrics: SSEConnectionMetrics) => void
}
