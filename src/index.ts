import { NitroModules } from 'react-native-nitro-modules'
import type {
  SSEClient as SSEClientSpec,
  SSEConnectionMetrics,
  SSEMessageEvent as NativeSSEMessageEvent,
  SSESessionOptions,
} from './specs/SSEClient.nitro'

export type {
  SSEConnectionMetrics,
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
 * `onMessage`/`onOpen`/`onError`/`onMetrics` callbacks; they all share the same underlying
 * URLSession/OkHttpClient, so reconnecting one doesn't disturb the others.
 */
export class SSEStream {
  private readonly native: SSEClientSpec =
    NitroModules.createHybridObject<SSEClientSpec>('SSEClient')

  set onMessage(callback: (event: SSEMessageEvent) => void) {
    this.native.onMessage = (event: NativeSSEMessageEvent) => {
      callback({ ...event, event: event.event ?? DEFAULT_MESSAGE_TYPE })
    }
  }

  set onOpen(callback: () => void) {
    this.native.onOpen = callback
  }

  set onError(callback: (message: string) => void) {
    this.native.onError = callback
  }

  set onMetrics(callback: (metrics: SSEConnectionMetrics) => void) {
    this.native.onMetrics = callback
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
    this.native.connect(url, options?.headers, session)
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
