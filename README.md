# react-native-nitro-sse-client

A native Server-Sent Events (SSE) client for React Native, built with [Nitro Modules](https://nitro.margelo.com) — no bridge, JSI-direct.

[![Version](https://img.shields.io/npm/v/react-native-nitro-sse-client.svg)](https://www.npmjs.com/package/react-native-nitro-sse-client)
[![License](https://img.shields.io/npm/l/react-native-nitro-sse-client.svg)](https://github.com/TaranVitalii/react-native-nitro-sse-client/blob/main/LICENSE)

## Why

Most React Native SSE clients (including the popular `react-native-sse`) are built on top of `XMLHttpRequest`. Every reconnect opens a brand new HTTP request from scratch, which means a full TCP + TLS handshake every time — typically 300–600ms of latency the user sees on every reconnect, even to a server they were just talking to a second ago.

This library talks to the platform's native HTTP stack directly — `URLSession` on iOS, `OkHttp` on Android — which both keep a warm connection pool. As long as the same client instance is reused across `connect()` calls (which it is, internally), a reconnect to the same host reuses the existing HTTP/2 connection instead of re-handshaking. `onMetrics` reports real, measured proof of this on every connection: whether it was a fresh handshake or a reused connection, and how long each phase took.

## Requirements

- React Native 0.76.0+ with the New Architecture enabled (Nitro Modules requires it)
- iOS 13+ / Android minSdk per `react-native-nitro-modules`
- `react-native-nitro-modules` installed as a direct dependency

## Installation

```sh
npm install react-native-nitro-sse-client react-native-nitro-modules
cd ios && pod install
```

## Usage

```ts
import { SSEClient } from 'react-native-nitro-sse-client'

SSEClient.onOpen = () => console.log('connected')

SSEClient.onMessage = event => {
  console.log(event.event, event.data) // event.id, event.event are optional per the SSE spec
}

SSEClient.onError = message => console.log('error:', message)

SSEClient.onMetrics = metrics => {
  if (metrics.phase === 'ttfb') {
    console.log(`time to first byte: ${metrics.ttfbMs}ms`)
  } else {
    // fires when the connection closes (disconnect(), or a superseding connect())
    console.log(
      `closed — reused=${metrics.connectionReused} ` +
        `connect=${metrics.connectMs}ms tls=${metrics.tlsMs}ms ttfb=${metrics.ttfbMs}ms`
    )
  }
}

SSEClient.connect('https://your-server.example.com/events')

// later
SSEClient.disconnect()
```

See [`example/App.tsx`](./example/App.tsx) for a complete, runnable example.

## API

`SSEClient` is a singleton `HybridObject` — v1 supports one connection at a time. Support for multiple independent client instances is planned; see [Roadmap](#roadmap).

### Methods

| Method | Description |
| --- | --- |
| `connect(url: string): void` | Opens a connection to `url`. Calling this while already connected cancels the previous connection first (its close metrics still fire). |
| `disconnect(): void` | Closes the current connection, if any. |

### Callbacks

Assign these as plain properties; they're invoked repeatedly, not one-shot.

| Callback | Fires |
| --- | --- |
| `onOpen: () => void` | When the server responds (response headers received). |
| `onMessage: (event: SSEMessageEvent) => void` | Once per parsed SSE event. |
| `onError: (message: string) => void` | On a network/transport failure. Not called for a `disconnect()` you initiated yourself. |
| `onMetrics: (metrics: SSEConnectionMetrics) => void` | Twice per connection — see below. |

### Types

```ts
interface SSEMessageEvent {
  id?: string
  event?: string
  data: string
  timestampMs: number
}

interface SSEConnectionMetrics {
  phase: 'ttfb' | 'closed'
  ttfbMs?: number
  dnsMs?: number
  connectMs?: number
  tlsMs?: number
  connectionReused?: boolean
  timestampMs: number
}
```

### Reading `onMetrics`

Each connection reports metrics twice:

1. **`phase: 'ttfb'`** — fires the moment the first byte of the body arrives. Only `ttfbMs` is populated; use this for a real-time "how long did that take" readout.
2. **`phase: 'closed'`** — fires once the connection ends (you called `disconnect()`, or a new `connect()` superseded it). This carries the authoritative breakdown: `connectMs`, `tlsMs`, and `connectionReused`.

`connectMs`/`tlsMs`/`dnsMs` come back `undefined` — not `0` — when `connectionReused` is `true`. That's not missing data: it means the OS handed the request an already-open connection, so those phases genuinely didn't happen. A `0`-vs-`undefined` distinction here is the actual signal, which is why the fields are optional rather than defaulting to zero.

`dnsMs` is currently always `undefined` on Android (OkHttp's `EventListener.dnsStart`/`dnsEnd` aren't wired up yet — PRs welcome) and on iOS depends on whether `URLSessionTaskMetrics` reports a distinct DNS phase for the connection.

## How reconnects work

Reconnecting is entirely manual: call `connect()` again (optionally after `disconnect()`) whenever you want to. There is no built-in auto-reconnect or backoff on top of the server's `retry:` field — this is intentional for v1, so nothing sits between you and the exact moment a reconnect happens. If you need resilience against dropped connections, drive `connect()`/`disconnect()` from your own retry logic (e.g. on `onError`).

## Roadmap

- Multiple concurrent client instances (currently a single module-level singleton)
- Custom headers / method / body on `connect()`
- `Last-Event-ID` resumption
- Optional built-in auto-reconnect with backoff, honoring the server's `retry:` field
- `dnsMs` on Android

Contributions toward any of these are welcome — see [Contributing](#contributing).

## How it's built

- **iOS**: a single `URLSession` (not `.shared`) created once and reused for every `connect()`/`disconnect()` cycle, so the connection pool persists across reconnects. SSE framing is parsed by hand from the streamed response body — no third-party SSE library — so nothing sits between this module and the exact reconnect timing. Handshake/TLS timings come from `URLSessionTaskMetrics`.
- **Android**: a single `OkHttpClient` created once, likewise reused across reconnects. Requests go through `client.newCall(request).enqueue(...)` with the response body read and parsed manually — **not** through `okhttp-sse`'s `EventSource`, because `RealEventSource.connect()` internally does `client.newBuilder().eventListener(...)`, which silently replaces any `eventListenerFactory` you set on the client, making handshake timing impossible to observe through it. Handshake/TLS timings come from OkHttp's `EventListener`.

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you'd like to change. See the [`example/`](./example) app to run and test the library in isolation.

## License

MIT
