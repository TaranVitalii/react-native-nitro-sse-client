import { NitroModules } from 'react-native-nitro-modules'
import type { SSEClient as SSEClientSpec } from './specs/SSEClient.nitro'

export type {
  SSEMessageEvent,
  SSEConnectionMetrics,
  SSEMetricsPhase,
  SSEClient as SSEClientSpec,
} from './specs/SSEClient.nitro'

export const SSEClient =
  NitroModules.createHybridObject<SSEClientSpec>('SSEClient')
