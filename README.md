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

stream.onError = message => console.log('error:', message)

// fires once, when the connection closes (disconnect(), a superseding connect(), or a failure)
stream.onMetrics = metrics => console.log('connection reused:', metrics.connectionReused)

stream.connect('https://your-server.example.com/events')

// later
stream.disconnect()
// or, once you're done with this stream entirely:
stream.destroy()
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
| `connect(url: string, options?: SSEConnectOptions): void` | Opens a connection to `url`. Calling this again on the same stream cancels the previous connection first (its close metrics still fire). |
| `disconnect(): void` | Closes the current connection, if any. |
| `destroy(): void` | Alias for `disconnect()` — call this when you're done with the stream (e.g. on unmount). |

### `SSEStream` callbacks

Assign these as plain properties; they're invoked repeatedly, not one-shot.

| Callback | Fires |
| --- | --- |
| `onOpen: () => void` | When the server responds (response headers received). |
| `onMessage: (event: SSEMessageEvent) => void` | Once per parsed SSE event. |
| `onError: (message: string) => void` | On a network/transport failure. Not called for a `disconnect()` you initiated yourself. |
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

interface SSEConnectOptions {
  session?: SSESessionOptions
}

interface SSESessionOptions {
  timeoutSeconds?: number
  maxConnectionsPerHost?: number
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

Reconnecting is entirely manual: call `connect()` again (optionally after `disconnect()`) whenever you want to. There is no built-in auto-reconnect or backoff on top of the server's `retry:` field — this is intentional for v1, so nothing sits between you and the exact moment a reconnect happens. If you need resilience against dropped connections, drive `connect()`/`disconnect()` from your own retry logic (e.g. on `onError`).

## Roadmap

- Custom headers / method / body on `connect()`
- `Last-Event-ID` resumption
- Optional built-in auto-reconnect with backoff, honoring the server's `retry:` field
- Per-event-type filtering (subscribe to a specific SSE `event:` type, like `react-native-sse-bridge-client`'s `addEventListener`)

Contributions toward any of these are welcome — see [Contributing](#contributing).

## How it's built

- **iOS**: one `URLSession` (not `.shared`), lazily created on the first `connect()` call across every `SSEStream` and reused for the app's lifetime, so the connection pool persists across reconnects and across instances. Each `SSEStream`'s `HybridSSEClient` keeps its own delegate object, assigned per-task via `URLSessionTask.delegate` (iOS 15+) — so multiple streams can be in flight at once, each routed to its own delegate, while still sharing one session/connection pool. SSE framing is parsed by hand, byte-level, from the streamed response body — no third-party SSE library. Handshake/TLS timings come from `URLSessionTaskMetrics`.
- **Android**: one `OkHttpClient`, likewise lazily created on the first `connect()` and shared across every `HybridSSEClient` instance/reconnect. Requests go through `client.newCall(request).enqueue(...)` with the response body read and parsed manually — **not** through `okhttp-sse`'s `EventSource`, because `RealEventSource.connect()` internally does `client.newBuilder().eventListener(...)`, which silently replaces any `eventListenerFactory` you set on the client, making handshake timing impossible to observe through it. OkHttp already creates one `EventListener` per call (not per client), so handshake/TLS timings are correctly attributed per stream even with a shared client; a request tag correlates each call back to its timing record.
- **Multiplexing**: `NitroModules.createHybridObject<SSEClient>('SSEClient')` gives every `SSEStream` its own native instance — unlike classic Native Modules, no `streamId`/event-filtering scheme is needed to keep multiple streams' events apart.

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you'd like to change. See the [`example/`](./example) app to run and test the library in isolation.

## License

MIT
