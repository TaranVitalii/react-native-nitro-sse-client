/**
 * Minimal usage example for react-native-nitro-sse-client.
 *
 * @format
 */

import React, { useCallback, useEffect, useState } from 'react'
import {
  FlatList,
  StatusBar,
  StyleSheet,
  Text,
  TextInput,
  TouchableOpacity,
  View,
  useColorScheme,
} from 'react-native'
import { SafeAreaProvider, SafeAreaView } from 'react-native-safe-area-context'
import { SSEClient } from 'react-native-nitro-sse-client'
import type { SSEConnectionMetrics, SSEMessageEvent } from 'react-native-nitro-sse-client'

const DEFAULT_URL = 'https://stream.wikimedia.org/v2/stream/recentchange'

interface LogEntry {
  id: number
  text: string
}

function formatMetrics(metrics: SSEConnectionMetrics): string {
  if (metrics.phase === 'ttfb') {
    return `time to first data: ${metrics.ttfbMs?.toFixed(1)}ms`
  }
  const fmt = (n?: number) => (n != null ? `${n.toFixed(1)}ms` : '-')
  return `closed — reused=${metrics.connectionReused} connect=${fmt(metrics.connectMs)} tls=${fmt(metrics.tlsMs)} ttfb=${fmt(metrics.ttfbMs)}`
}

function App(): React.JSX.Element {
  const isDarkMode = useColorScheme() === 'dark'
  const [url, setUrl] = useState(DEFAULT_URL)
  const [connected, setConnected] = useState(false)
  const [entries, setEntries] = useState<LogEntry[]>([])

  const log = useCallback((text: string) => {
    setEntries(prev => [{ id: prev.length, text }, ...prev].slice(0, 200))
  }, [])

  useEffect(() => {
    SSEClient.onOpen = () => log('opened')
    SSEClient.onMessage = (event: SSEMessageEvent) => {
      log(`message: ${event.data.slice(0, 100)}`)
    }
    SSEClient.onError = (message: string) => log(`error: ${message}`)
    SSEClient.onMetrics = (metrics: SSEConnectionMetrics) => log(formatMetrics(metrics))

    return () => {
      SSEClient.disconnect()
    }
  }, [log])

  const handleConnect = () => {
    log(`connecting to ${url}`)
    SSEClient.connect(url)
    setConnected(true)
  }

  const handleDisconnect = () => {
    SSEClient.disconnect()
    setConnected(false)
    log('disconnected')
  }

  return (
    <SafeAreaProvider>
      <StatusBar barStyle={isDarkMode ? 'light-content' : 'dark-content'} />
      <SafeAreaView style={styles.container} edges={['top', 'bottom']}>
        <Text style={styles.title}>react-native-nitro-sse-client</Text>
        <TextInput
          style={styles.input}
          value={url}
          onChangeText={setUrl}
          autoCapitalize="none"
          autoCorrect={false}
          editable={!connected}
        />
        <View style={styles.row}>
          <TouchableOpacity
            style={[styles.button, connected && styles.buttonDisabled]}
            onPress={handleConnect}
            disabled={connected}>
            <Text style={styles.buttonText}>Connect</Text>
          </TouchableOpacity>
          <TouchableOpacity
            style={[styles.button, !connected && styles.buttonDisabled]}
            onPress={handleDisconnect}
            disabled={!connected}>
            <Text style={styles.buttonText}>Disconnect</Text>
          </TouchableOpacity>
        </View>
        <FlatList
          style={styles.log}
          data={entries}
          keyExtractor={item => String(item.id)}
          renderItem={({ item }) => <Text style={styles.logLine}>{item.text}</Text>}
        />
      </SafeAreaView>
    </SafeAreaProvider>
  )
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    padding: 12,
  },
  title: {
    fontSize: 16,
    fontWeight: '700',
    marginBottom: 8,
  },
  input: {
    borderWidth: 1,
    borderColor: '#c4c9d0',
    borderRadius: 6,
    paddingHorizontal: 10,
    paddingVertical: 8,
    fontSize: 13,
    marginBottom: 8,
  },
  row: {
    flexDirection: 'row',
    gap: 8,
    marginBottom: 8,
  },
  button: {
    flex: 1,
    backgroundColor: '#1f6feb',
    borderRadius: 6,
    paddingVertical: 10,
    alignItems: 'center',
  },
  buttonDisabled: {
    backgroundColor: '#c4c9d0',
  },
  buttonText: {
    color: 'white',
    fontWeight: '600',
  },
  log: {
    flex: 1,
    backgroundColor: '#0d1117',
    borderRadius: 6,
    padding: 8,
  },
  logLine: {
    fontFamily: 'Menlo',
    fontSize: 11,
    color: '#c9d1d9',
    marginBottom: 2,
  },
})

export default App
