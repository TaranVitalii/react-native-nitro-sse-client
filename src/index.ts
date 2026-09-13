import { NitroModules } from 'react-native-nitro-modules'
import type {
  SSEClient as SSEClientSpec,
  SSEConnectionMetrics,
  SSEConnectionState,
  SSEError,
  SSEMessageEvent as NativeSSEMessageEvent,
  SSEReconnectOptions,
  SSESessionOptions,
} from './specs/SSEClient.nitro'

export type {
  SSEConnectionMetrics,
  SSEConnectionState,
  SSEError,
  SSEErrorType,
  SSEReconnectOptions,
  SSESessionOptions,
  SSEClient as SSEClientSpec,
} from './specs/SSEClient.nitro'

// Resolved-`event` variant of the native SSEMessageEvent (which leaves `event` undefined for a
// frame with no `event:` field) — normalized to 'message' here to match how browser EventSource,
// and react-native-sse-bridge-client, both treat a typeless frame.
export interface SSEMessageEvent {
  id?: string
  event: string
  data: string
  timestampMs: number
}

// SSE frames with no `event:` field are filed under this key, matching how browser EventSource
// (and react-native-sse-bridge-client) treats them as type 'message'.
const DEFAULT_MESSAGE_TYPE = 'message'

export interface SSEConnectOptions {
  headers?: Record<string, string>
  /** Only takes effect on the first connect() made across all streams — see configureSSESession(). */
  session?: SSESessionOptions
  /** Automatic reconnect after the connection ends (error, or the server closing the stream).
   * On by default — see SSEReconnectOptions. */
  reconnect?: SSEReconnectOptions
  /** Default 'GET'. Use 'POST' (with `body`) for APIs that stream SSE responses to a request
   * body — e.g. most LLM chat-completion endpoints. */
  method?: string
  /** Sent as the raw request body (e.g. `JSON.stringify(...)`. Set your own `Content-Type` via
   * `headers` — none is assumed. */
  body?: string
  /** Default true. A non-`text/event-stream` Content-Type on an otherwise-successful response is
   * reported via onError (type 'invalid-content-type') instead of being treated as open. Set
   * false for a server that's valid SSE but sends a different/no Content-Type. */
  validateContentType?: boolean
}

let sharedSessionCreated = false
// Set by configureSSESession(), applied to whichever connect() call ends up being the first one
// across every SSEStream in the app — see configureSSESession() below.
let defaultSessionOptions: SSESessionOptions | undefined

/**
 * Sets the shared session config (timeout, max connections per host) once, up front — call this
 * at app startup (e.g. in App.tsx, before any screen creates/connects a stream) instead of
 * passing `session` to whichever connect() happens to run first. Every stream's connect() then
 * uses this as its default (an explicit `session` passed to connect() still wins for that call).
 *
 * Must be called before the first connect() anywhere in the app — the underlying URLSession/
 * OkHttpClient is created lazily on that call, so calling this any later has nothing left to
 * configure and logs a warning instead of silently doing nothing.
 */
export function configureSSESession(options: SSESessionOptions): void {
  if (sharedSessionCreated) {
    console.warn(
      '[react-native-nitro-sse-client] configureSSESession() was called after a stream had ' +
        'already connected — the shared session/client already exists, so these options have ' +
        'no effect. Call configureSSESession() once at app startup, before creating or ' +
        'connecting any SSEStream.'
    )
    return
  }
  defaultSessionOptions = options
}

/**
 * One independently connect()-able/disconnect()-able SSE stream, backed by its own Nitro
 * HybridObject instance. Create as many as you need — each gets its own connection and its own
 * `onMessage`/`onOpen`/`onError`/`onClose`/`onMetrics` callbacks; they all share the same
 * underlying URLSession/OkHttpClient, so reconnecting one doesn't disturb the others.
 *
 * Reconnects automatically after the connection ends for any reason other than disconnect()
 * (mirrors browser EventSource / react-native-sse) — disable via `connect(url, { reconnect: {
 * enabled: false } })` if you'd rather handle that yourself.
 */
export class SSEStream {
  private readonly native: SSEClientSpec =
    NitroModules.createHybridObject<SSEClientSpec>('SSEClient')

  // Tracked internally (regardless of whether the caller ever sets onStateChange) so getState()
  // has an answer synchronously, without a round-trip to native.
  private cachedState: SSEConnectionState = 'idle'
  private userOnStateChange?: (state: SSEConnectionState) => void

  constructor() {
    this.native.onStateChange = (state: SSEConnectionState) => {
      this.cachedState = state
      this.userOnStateChange?.(state)
    }
  }

  set onMessage(callback: (event: SSEMessageEvent) => void) {
    this.native.onMessage = (event: NativeSSEMessageEvent) => {
      callback({ ...event, event: event.event ?? DEFAULT_MESSAGE_TYPE })
    }
  }

  set onOpen(callback: () => void) {
    this.native.onOpen = callback
  }

  set onError(callback: (error: SSEError) => void) {
    this.native.onError = callback
  }

  /** Fires whenever the connection ends, for any reason — a normal server-side close, an error
   * (right after onError), or an explicit disconnect(). */
  set onClose(callback: () => void) {
    this.native.onClose = callback
  }

  set onMetrics(callback: (metrics: SSEConnectionMetrics) => void) {
    this.native.onMetrics = callback
  }

  /** Fires on every connection-state transition — see SSEConnectionState. */
  set onStateChange(callback: (state: SSEConnectionState) => void) {
    this.userOnStateChange = callback
  }

  /** Awaited immediately before every request this stream makes — the initial connect() and
   * every automatic reconnect alike — so it's the right place to refresh a short-lived auth
   * token rather than letting a reconnect fire with a stale one. Whatever headers it resolves
   * with are merged over the connect()-time headers (resolved values win on a key collision). */
  set onBeforeRequest(callback: () => Promise<Record<string, string>>) {
    this.native.onBeforeRequest = callback
  }

  /** The stream's current connection state — see SSEConnectionState. Always up to date; doesn't
   * require an onStateChange listener to be registered. */
  getState(): SSEConnectionState {
    return this.cachedState
  }

  connect(url: string, options?: SSEConnectOptions): void {
    // An explicit `session` on this call wins; otherwise fall back to whatever
    // configureSSESession() set at app startup, if anything.
    const session = options?.session ?? defaultSessionOptions
    // Only warn when THIS call explicitly passed `session` and it's too late for it to apply —
    // silently falling back to the app-wide default (or to nothing) on a later stream is the
    // normal, expected case, not a mistake worth flagging.
    if (options?.session && sharedSessionCreated) {
      console.warn(
        '[react-native-nitro-sse-client] `session` options were ignored: the shared session/' +
          'client was already created by an earlier connect() call (on this or another ' +
          'SSEStream). Session config only takes effect on the very first connect() made ' +
          'across the whole app — call configureSSESession() once at startup instead. See the ' +
          'README for details.'
      )
    }
    sharedSessionCreated = true
    this.native.connect(
      url,
      options?.headers,
      session,
      options?.reconnect,
      options?.method,
      options?.body,
      options?.validateContentType
    )
  }

  disconnect(): void {
    this.native.disconnect()
  }

  /** Alias for disconnect() — kept for parity with react-native-sse-bridge-client's SSEStream. */
  destroy(): void {
    this.disconnect()
  }
}

export function createSSEStream(): SSEStream {
  return new SSEStream()
}
