import type { HybridObject } from 'react-native-nitro-modules'

export type SSEMetricsPhase = 'ttfb' | 'closed'

export interface SSEMessageEvent {
  id?: string
  event?: string
  data: string
  timestampMs: number
}

export interface SSEConnectionMetrics {
  phase: SSEMetricsPhase
  ttfbMs?: number
  dnsMs?: number
  connectMs?: number
  tlsMs?: number
  connectionReused?: boolean
  timestampMs: number
}

export interface SSEClient
  extends HybridObject<{ ios: 'swift'; android: 'kotlin' }> {
  connect(url: string): void
  disconnect(): void
  onMessage: (event: SSEMessageEvent) => void
  onOpen: () => void
  onError: (message: string) => void
  onMetrics: (metrics: SSEConnectionMetrics) => void
}
