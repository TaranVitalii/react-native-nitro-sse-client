//
//  HybridSSEClient.swift
//  nitro-sse-client
//

import Foundation
import NitroModules

enum SSEClientError: Error {
  case invalidURL
}

/// Forwards URLSession delegate callbacks to HybridSSEClient. A separate NSObject subclass is
/// required because URLSessionDataDelegate requires NSObjectProtocol conformance, which the
/// Nitro-generated HybridSSEClientSpec base class does not provide.
private final class SSEStreamDelegate: NSObject, URLSessionDataDelegate {
  weak var client: HybridSSEClient?

  func urlSession(
    _ session: URLSession,
    dataTask: URLSessionDataTask,
    didReceive response: URLResponse,
    completionHandler: @escaping (URLSession.ResponseDisposition) -> Void
  ) {
    client?.handleResponse()
    completionHandler(.allow)
  }

  func urlSession(_ session: URLSession, dataTask: URLSessionDataTask, didReceive data: Data) {
    client?.handleData(data)
  }

  func urlSession(_ session: URLSession, task: URLSessionTask, didCompleteWithError error: Error?) {
    client?.handleCompletion(error: error)
  }

  func urlSession(_ session: URLSession, task: URLSessionTask, didFinishCollecting metrics: URLSessionTaskMetrics) {
    client?.handleMetrics(metrics)
  }
}

class HybridSSEClient: HybridSSEClientSpec {
  var onMessage: (SSEMessageEvent) -> Void = { _ in }
  var onOpen: () -> Void = { }
  var onError: (String) -> Void = { _ in }
  var onMetrics: (SSEConnectionMetrics) -> Void = { _ in }

  // Created once and reused for every connect()/disconnect() cycle so the underlying
  // connection pool (and any HTTP/2 session / TLS session tickets) survives reconnects.
  private let session: URLSession
  private let streamDelegate = SSEStreamDelegate()

  private var currentTask: URLSessionDataTask?
  private var textBuffer = ""
  private var connectStartedAt: Date?
  private var firstByteLogged = false

  public override init() {
    let config = URLSessionConfiguration.default
    config.timeoutIntervalForRequest = 3600
    config.httpMaximumConnectionsPerHost = 6
    self.session = URLSession(configuration: config, delegate: streamDelegate, delegateQueue: nil)
    super.init()
    streamDelegate.client = self
  }

  func connect(url: String) throws {
    guard let nsUrl = URL(string: url) else { throw SSEClientError.invalidURL }

    // Cancelling here (rather than tearing down `session`) is what lets connection N+1 reuse
    // the pool built up by connection N — only the task is torn down, never the session.
    currentTask?.cancel()

    textBuffer = ""
    firstByteLogged = false
    connectStartedAt = Date()

    var request = URLRequest(url: nsUrl)
    request.setValue("text/event-stream", forHTTPHeaderField: "Accept")
    // Some SSE providers (e.g. Wikimedia) reject requests carrying the platform's generic
    // default User-Agent; identify this library instead of leaving it unset.
    request.setValue(
      "react-native-nitro-sse-client (+https://github.com/TaranVitalii/react-native-nitro-sse-client)",
      forHTTPHeaderField: "User-Agent"
    )
    request.timeoutInterval = 3600

    let task = session.dataTask(with: request)
    currentTask = task
    task.resume()
  }

  func disconnect() throws {
    currentTask?.cancel()
    currentTask = nil
  }

  fileprivate func handleResponse() {
    onOpen()
  }

  fileprivate func handleData(_ data: Data) {
    if !firstByteLogged, let startedAt = connectStartedAt {
      firstByteLogged = true
      let ttfbMs = Date().timeIntervalSince(startedAt) * 1000
      NSLog("[Native SSE][iOS] time to first data: %.1fms", ttfbMs)
      onMetrics(SSEConnectionMetrics(
        phase: .ttfb,
        ttfbMs: ttfbMs,
        dnsMs: nil,
        connectMs: nil,
        tlsMs: nil,
        connectionReused: nil,
        timestampMs: Date().timeIntervalSince1970 * 1000
      ))
    }

    guard let chunk = String(data: data, encoding: .utf8) else { return }
    textBuffer += chunk.replacingOccurrences(of: "\r\n", with: "\n")
    drainBuffer()
  }

  fileprivate func handleCompletion(error: Error?) {
    guard let error = error as NSError? else { return }
    if error.code == NSURLErrorCancelled { return }
    NSLog("[Native SSE][iOS] error: %@", error.localizedDescription)
    onError(error.localizedDescription)
  }

  fileprivate func handleMetrics(_ metrics: URLSessionTaskMetrics) {
    guard let txn = metrics.transactionMetrics.last else { return }

    // Pooled/reused connections skip the connect+TLS phases entirely, so these dates come
    // back nil — that absence (not a separate flag) is the actual reuse signal.
    let reused = txn.connectStartDate == nil

    func durationMs(_ start: Date?, _ end: Date?) -> Double? {
      guard let start, let end else { return nil }
      return end.timeIntervalSince(start) * 1000
    }

    let dnsMs = durationMs(txn.domainLookupStartDate, txn.domainLookupEndDate)
    let connectMs = durationMs(txn.connectStartDate, txn.connectEndDate)
    let tlsMs = durationMs(txn.secureConnectionStartDate, txn.secureConnectionEndDate)
    let ttfbMs = durationMs(txn.fetchStartDate, txn.responseStartDate)

    NSLog(
      "[Native SSE][iOS] connection closed reused=%@ connect=%.1fms tls=%.1fms ttfb=%.1fms",
      reused ? "true" : "false", connectMs ?? 0, tlsMs ?? 0, ttfbMs ?? 0
    )

    onMetrics(SSEConnectionMetrics(
      phase: .closed,
      ttfbMs: ttfbMs,
      dnsMs: dnsMs,
      connectMs: connectMs,
      tlsMs: tlsMs,
      connectionReused: reused,
      timestampMs: Date().timeIntervalSince1970 * 1000
    ))
  }

  private func drainBuffer() {
    while let range = textBuffer.range(of: "\n\n") {
      let rawEvent = String(textBuffer[textBuffer.startIndex..<range.lowerBound])
      textBuffer.removeSubrange(textBuffer.startIndex..<range.upperBound)
      parseAndEmit(rawEvent)
    }
  }

  private func parseAndEmit(_ rawEvent: String) {
    var id: String?
    var eventName: String?
    var dataLines: [String] = []

    for line in rawEvent.split(separator: "\n", omittingEmptySubsequences: false) {
      if line.hasPrefix(":") { continue }
      if line.hasPrefix("id:") {
        id = String(line.dropFirst(3)).trimmingCharacters(in: .whitespaces)
      } else if line.hasPrefix("event:") {
        eventName = String(line.dropFirst(6)).trimmingCharacters(in: .whitespaces)
      } else if line.hasPrefix("data:") {
        dataLines.append(String(line.dropFirst(5)).trimmingCharacters(in: .whitespaces))
      }
    }

    guard !dataLines.isEmpty else { return }

    onMessage(SSEMessageEvent(
      id: id,
      event: eventName,
      data: dataLines.joined(separator: "\n"),
      timestampMs: Date().timeIntervalSince1970 * 1000
    ))
  }
}
