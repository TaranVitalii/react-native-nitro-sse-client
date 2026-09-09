# react-native-nitro-sse-client

A native Server-Sent Events (SSE) client for React Native, built with [Nitro Modules](https://nitro.margelo.com) — no bridge, JSI-direct.

[![Version](https://img.shields.io/npm/v/react-native-nitro-sse-client.svg)](https://www.npmjs.com/package/react-native-nitro-sse-client)
[![License](https://img.shields.io/npm/l/react-native-nitro-sse-client.svg)](https://github.com/TaranVitalii/react-native-nitro-sse-client/blob/main/LICENSE)

## Why

Most React Native SSE clients (including the popular `react-native-sse`) are built on top of `XMLHttpRequest`. Every reconnect opens a brand new HTTP request from scratch, which means a full TCP + TLS handshake every time — typically 300–600ms of latency the user sees on every reconnect, even to a server they were just talking to a second ago.

This library talks to the platform's native HTTP stack directly — `URLSession` on iOS, `OkHttp` on Android — which both keep a warm connection pool. As long as the same underlying client is reused across `connect()` calls (which it is, internally), a reconnect to the same host reuses the existing HTTP/2 connection instead of re-handshaking. `onMetrics` reports real, measured proof of this on every connection: whether it was a fresh handshake or a reused one.

It's built on Nitro Modules — JSI-direct, no bridge — so every `SSEStream` you create is its own native `HybridObject` instance. If your app can't use the New Architecture, see [`react-native-sse-bridge-client`](https://github.com/TaranVitalii/react-native-sse-bridge-client) instead — same idea, built on the classic Native Modules bridge.

## Requirements

- React Native 0.76.0+ with the New Architecture enabled (Nitro Modules requires it)
- iOS 15.1+ / Android minSdk per `react-native-nitro-modules`
- `react-native-nitro-modules` installed as a direct dependency

## Installation

```sh
npm install react-native-nitro-sse-client react-native-nitro-modules
cd ios && pod install
```

## Usage

```ts
import { createSSEStream } from 'react-native-nitro-sse-client'

const stream = createSSEStream()

stream.onOpen = () => console.log('connected')

stream.onMessage = event => {
  console.log(event.event, event.data) // event.id, event.event are optional per the SSE spec
}

// error.type is 'http' | 'network' | 'timeout' | 'exception'; error.statusCode is set for 'http'
stream.onError = error => console.log('error:', error.type, error.message)

// fires whenever the connection ends, for any reason — including the automatic reconnect this
// library does by default, so this doesn't mean the stream gave up
stream.onClose = () => console.log('closed')

// fires once, when the connection closes (disconnect(), a superseding connect(), or a failure)
stream.onMetrics = metrics => console.log('connection reused:', metrics.connectionReused)

stream.connect('https://your-server.example.com/events', {
  headers: { Authorization: 'Bearer …' },
})

// later
stream.disconnect()
// or, once you're done with this stream entirely:
stream.destroy()
```

### POST requests

Some SSE APIs — most LLM chat-completion endpoints included — stream the response to a `POST`
whose body carries the request payload, rather than a plain `GET`. Pass `method`/`body`:

```ts
stream.connect('https://your-server.example.com/chat/completions', {
  method: 'POST',
  headers: {
    Authorization: 'Bearer …',
    'Content-Type': 'application/json',
  },
  body: JSON.stringify({ model: 'your-model', messages, stream: true }),
})
```

See [`example/App.tsx`](./example/App.tsx) for a complete, runnable example.

## API

### `createSSEStream(): SSEStream`

Creates a new, independent stream — each one is its own native `HybridObject` instance, with its own connection and its own callbacks. Create as many as you need; they share the underlying `URLSession`/`OkHttpClient`, so reconnecting one doesn't disturb the others.

### `configureSSESession(options: SSESessionOptions): void`

Sets the shared session config (timeout, max connections per host) once, up front. Call this at app startup, before any stream connects — see [Configuring the shared session](#configuring-the-shared-session) below.

### `SSEStream` methods

| Method | Description |
| --- | --- |
| `connect(url: string, options?: SSEConnectOptions): void` | Opens a connection to `url`, `GET` by default — pass `options.method`/`options.body` for a POST-based SSE API (most LLM chat-completion endpoints). Calling this again on the same stream cancels the previous connection first (its close metrics still fire). Headers — including `Authorization` — are entirely JS-configured; nothing is hardcoded natively beyond a default `Accept`/`User-Agent`, both of which `options.headers` can override too. |
| `disconnect(): void` | Closes the current connection, if any. |
| `destroy(): void` | Alias for `disconnect()` — call this when you're done with the stream (e.g. on unmount). |

### `SSEStream` callbacks

Assign these as plain properties; they're invoked repeatedly, not one-shot.

| Callback | Fires |
| --- | --- |
| `onOpen: () => void` | When the server responds with a successful (2xx) status. |
| `onMessage: (event: SSEMessageEvent) => void` | Once per parsed SSE event. |
| `onError: (error: SSEError) => void` | On a non-2xx HTTP response, a Content-Type mismatch (unless `validateContentType: false`), a network/transport failure, a timeout, or an invalid URL — see [Types](#types) below for `SSEError`. Not called for a `disconnect()` you initiated yourself. |
| `onClose: () => void` | Whenever the connection ends, for any reason — a normal server-side close, right after `onError`, or an explicit `disconnect()`. Fires again after every automatic reconnect's connection ends, so it does not mean the stream gave up. |
| `onMetrics: (metrics: SSEConnectionMetrics) => void` | Fires once per connection, when it ends — see [Reading `onMetrics`](#reading-onmetrics) below. |

### Types

```ts
interface SSEMessageEvent {
  id?: string
  event?: string
  data: string
  timestampMs: number
}

interface SSEConnectionMetrics {
  connectionReused: boolean
}

// 'http': non-2xx response — statusCode and message (the response body) are populated.
// 'invalid-content-type': a 2xx response whose Content-Type wasn't text/event-stream (only
// reported when validateContentType is true, the default) — message describes what was received.
// 'timeout': the request's own timeout (SSESessionOptions.timeoutSeconds) elapsed.
// 'network': a transport-level failure (DNS, connection refused, TLS, dropped connection, etc.).
// 'exception': the call couldn't even be attempted (e.g. an invalid URL).
type SSEErrorType = 'http' | 'invalid-content-type' | 'network' | 'timeout' | 'exception'

interface SSEError {
  message: string
  type: SSEErrorType
  statusCode?: number // only set when type is 'http'
}

interface SSEConnectOptions {
  headers?: Record<string, string>
  session?: SSESessionOptions
  reconnect?: SSEReconnectOptions
  method?: string // default 'GET'; use 'POST' (with `body`) for APIs that stream to a request body
  body?: string // raw request body (e.g. JSON.stringify(...)); set Content-Type via `headers`
  validateContentType?: boolean // default true — see SSEErrorType 'invalid-content-type' above
}

interface SSESessionOptions {
  timeoutSeconds?: number
  maxConnectionsPerHost?: number
}

interface SSEReconnectOptions {
  enabled?: boolean // default true
  intervalMs?: number // default 3000; overridden per-stream by a server `retry:` field
  maxAttempts?: number // default undefined (retry forever); resets to 0 after a successful onOpen
  // Default false. A 4xx response or a Content-Type mismatch does NOT trigger a reconnect by
  // default (except 429, which always retries) — that class of failure usually means retrying
  // identically won't help. Set true to retry every HTTP error, including 4xx.
  retryOnClientError?: boolean
}
```

### Configuring the shared session

The underlying `URLSession`/`OkHttpClient` is shared by every `SSEStream` in the app and, once created, kept alive for the app's lifetime — that's the whole mechanism behind connection reuse. Because of that, this config only takes effect once, on whichever `connect()` call ends up being the very first one made across the whole app; after that, the shared client already exists and any further attempt to set it is a no-op.

Rather than relying on "whichever stream happens to connect first" and passing `session` there, call `configureSSESession()` once at app startup — e.g. at the top of `App.tsx`, before any screen creates a stream:

```ts
// App.tsx
import { configureSSESession } from 'react-native-nitro-sse-client'

configureSSESession({ timeoutSeconds: 1800, maxConnectionsPerHost: 4 })

export default function App() {
  // screens create/connect their own streams from here on, all sharing this config
  ...
}
```

Every stream's `connect()` then picks this up automatically as its default. You can still pass `session` directly to a particular `connect()` call if you want that one call to override the app-wide default (it only actually applies if that call turns out to be the first one, same rule as above — a `console.warn` fires if it doesn't).

```ts
// lower-level escape hatch — same one-time-effect caveat as configureSSESession()
stream.connect(url, {
  session: { timeoutSeconds: 1800, maxConnectionsPerHost: 4 },
})
```

| Option | iOS | Android |
| --- | --- | --- |
| `timeoutSeconds` | `URLSessionConfiguration.timeoutIntervalForRequest` (default `3600`) | OkHttp `readTimeout` (default `0`, i.e. unlimited) |
| `maxConnectionsPerHost` | `URLSessionConfiguration.httpMaximumConnectionsPerHost` (default `6`) | OkHttp `Dispatcher.maxRequestsPerHost` — the closest equivalent; HTTP/2 hosts multiplex many requests over one connection regardless (default `5`, OkHttp's own default) |

### Reading `onMetrics`

Fires once per connection, when it ends (you called `disconnect()`, a new `connect()` superseded it, or it failed) — `connectionReused` is only knowable at that point, not any earlier. On iOS it comes from `URLSessionTaskMetrics`, which the OS only hands over once the task has fully finished; there's no way to report this alongside `onOpen` on either platform.

`connectionReused: true` means the OS handed this connection an already-open TCP/TLS session from the pool instead of doing a fresh handshake — the thing this whole library exists to make happen. `false` on every reconnect to the same host would mean something's wrong (a new `URLSession`/`OkHttpClient` being created somewhere, a host header mismatch, etc.).

A full per-phase timing breakdown (DNS/connect/TLS/TTFB) is still logged natively (`NSLog` on iOS, `Log.d` on Android) for whoever's debugging the library itself — it's just not sent across JSI, since a granular breakdown isn't something most consumers of the library need.

## How reconnects work

Reconnecting is automatic by default, mirroring the browser `EventSource` model (and `react-native-sse`): whenever a connection ends for any reason other than your own `disconnect()` — a non-2xx response, a network/timeout error, or the server just closing the stream normally — the stream reconnects to the same URL after a delay.

- **Delay**: starts at `reconnect.intervalMs` (default `3000`). A `retry:` field in the stream overrides it for that stream's *next* reconnects — there's no exponential backoff on top of that, matching `react-native-sse`'s behavior.
- **`Last-Event-ID`**: if any received event had an `id:` field, it's sent as the `Last-Event-ID` header on the next automatic reconnect, so a server that supports it can resume from where it left off. An explicit `connect()` call always starts a fresh logical session — it does not send a stale `Last-Event-ID` from before.
- **Giving up**: set `reconnect.maxAttempts` to stop retrying after that many consecutive failures (default: retry forever). The counter resets to 0 after any successful `onOpen`.
- **Client errors**: a 4xx response or a Content-Type mismatch (see `validateContentType`) does **not** trigger a reconnect by default — retrying an identical request against a 401/403/404/etc. usually just repeats the same failure. The one default exception is `429` (rate limited), which always retries. Set `reconnect.retryOnClientError: true` to retry every HTTP error, including 4xx. 5xx, network, and timeout errors always retry (subject to `maxAttempts`), regardless of this setting.
- **Opting out**: `stream.connect(url, { reconnect: { enabled: false } })` disables it entirely — call `connect()` yourself (e.g. from `onError`/`onClose`) to drive reconnection your own way.

```ts
stream.connect(url, {
  reconnect: { intervalMs: 5000, maxAttempts: 5 },
})
```

## How it's built

- **iOS**: one `URLSession` (not `.shared`), lazily created on the first `connect()` call across every `SSEStream` and reused for the app's lifetime, so the connection pool persists across reconnects and across instances. Each `SSEStream`'s `HybridSSEClient` keeps its own delegate object, assigned per-task via `URLSessionTask.delegate` (iOS 15+) — so multiple streams can be in flight at once, each routed to its own delegate, while still sharing one session/connection pool. SSE framing is parsed by hand, byte-level, from the streamed response body — no third-party SSE library. Handshake/TLS timings come from `URLSessionTaskMetrics`.
- **Android**: one `OkHttpClient`, likewise lazily created on the first `connect()` and shared across every `HybridSSEClient` instance/reconnect. Requests go through `client.newCall(request).enqueue(...)` with the response body read and parsed manually — **not** through `okhttp-sse`'s `EventSource`, because `RealEventSource.connect()` internally does `client.newBuilder().eventListener(...)`, which silently replaces any `eventListenerFactory` you set on the client, making handshake timing impossible to observe through it. OkHttp already creates one `EventListener` per call (not per client), so handshake/TLS timings are correctly attributed per stream even with a shared client; a request tag correlates each call back to its timing record.
- **Multiplexing**: `NitroModules.createHybridObject<SSEClient>('SSEClient')` gives every `SSEStream` its own native instance — unlike classic Native Modules, no `streamId`/event-filtering scheme is needed to keep multiple streams' events apart.

## License

MIT
