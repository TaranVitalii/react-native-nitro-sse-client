//
//  HybridSSEClient.swift
//  nitro-sse-client
//

import Foundation
import NitroModules

enum SSEClientError: Error {
  case invalidURL
}

/// Forwards URLSession delegate callbacks to the HybridSSEClient instance that owns the task.
/// Assigned per-task (`URLSessionTask.delegate`, available iOS 15+ — this library already
/// requires RN 0.76+ / the New Architecture, which requires iOS 15.1+) instead of at the session
/// level, so every instance keeps its own delegate — routed straight back to `self`, no manual
/// task-ID bookkeeping needed — while still sharing one `URLSession` underneath for pooling.
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

/// Shared by every HybridSSEClient instance — created once and reused for every connect()/
/// disconnect() cycle across ALL instances (not just reconnects on the same one), so the
/// connection pool (and any HTTP/2 session / TLS session tickets) persists no matter which
/// instance is talking to a given host.
private enum SharedSession {
  static var session: URLSession?

  // Lazily created from whichever connect() call comes first, across every HybridSSEClient
  // instance in the app — later calls' `session` options are silently ignored once this exists,
  // since recreating it would drop every connection already in the pool.
  static func get(_ options: SSESessionOptions?) -> URLSession {
    if let session { return session }

    let config = URLSessionConfiguration.default
    config.timeoutIntervalForRequest = options?.timeoutSeconds ?? 3600
    config.httpMaximumConnectionsPerHost = options?.maxConnectionsPerHost.map { Int($0) } ?? 6

    // No session-level delegate — every task gets its own via `task.delegate =` in connect(),
    // so there's nothing here that needs to route between instances.
    let newSession = URLSession(configuration: config, delegate: nil, delegateQueue: nil)
    session = newSession
    return newSession
  }
}

class HybridSSEClient: HybridSSEClientSpec {
  var onMessage: (SSEMessageEvent) -> Void = { _ in }
  var onOpen: () -> Void = { }
  var onError: (String) -> Void = { _ in }
  var onMetrics: (SSEConnectionMetrics) -> Void = { _ in }

  private let streamDelegate = SSEStreamDelegate()

  private var currentTask: URLSessionDataTask?
  // Raw bytes rather than a Swift String: String's range(of:)/+= are Unicode-grapheme-aware and
  // re-scan/re-copy the whole buffer on every call, too slow to do on every network chunk for a
  // chatty stream — this buffer is only ever decoded to a String once a full frame is isolated.
  private var byteBuffer = Data()
  private static let frameDelimiter = Data([0x0A, 0x0A]) // "\n\n"
  private var connectStartedAt: Date?
  private var firstByteLogged = false

  public override init() {
    super.init()
    streamDelegate.client = self
  }

  func connect(url: String, headers: [String: String]?, session: SSESessionOptions?) throws {
    guard let nsUrl = URL(string: url) else { throw SSEClientError.invalidURL }

    // Cancelling here (rather than tearing down the shared session) is what lets connection N+1
    // reuse the pool built up by connection N — only the task is torn down, never the session.
    currentTask?.cancel()

    byteBuffer.removeAll(keepingCapacity: false)
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
    // Applied after the defaults above, so a caller can override Accept/User-Agent too if they
    // need to — e.g. Authorization for a protected endpoint.
    if let headers {
      for (key, value) in headers {
        request.setValue(value, forHTTPHeaderField: key)
      }
    }

    let sharedSession = SharedSession.get(session)
    // URLSessionConfiguration.timeoutIntervalForRequest is unreliable once a request carries its
    // own timeoutInterval (which URLRequest always does, defaulting to 60s) — the request-level
    // value wins. Reading it back off the session's own configuration, rather than hardcoding a
    // separate constant here, keeps this in sync with whatever configureSSESession() set.
    request.timeoutInterval = sharedSession.configuration.timeoutIntervalForRequest

    let task = sharedSession.dataTask(with: request)
    task.delegate = streamDelegate
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
      // Native-only diagnostic — not sent to JS. onMetrics is limited to connectionReused (see
      // SSEConnectionMetrics), which isn't knowable until the connection closes.
      let ttfbMs = Date().timeIntervalSince(startedAt) * 1000
      NSLog("[Native SSE][iOS] time to first data: %.1fms", ttfbMs)
    }

    // Drop bare CR bytes so "\r\n" collapses to "\n" (SSE line endings), without the cost of
    // decoding to a String just to normalize — same effect, byte-level, once per chunk.
    byteBuffer.append(data.filter { $0 != 0x0D })
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

    let connectMs = durationMs(txn.connectStartDate, txn.connectEndDate)
    let tlsMs = durationMs(txn.secureConnectionStartDate, txn.secureConnectionEndDate)
    let ttfbMs = durationMs(txn.fetchStartDate, txn.responseStartDate)

    // Full breakdown stays native-only (log line) — only connectionReused crosses the bridge.
    NSLog(
      "[Native SSE][iOS] connection closed reused=%@ connect=%.1fms tls=%.1fms ttfb=%.1fms",
      reused ? "true" : "false", connectMs ?? 0, tlsMs ?? 0, ttfbMs ?? 0
    )

    onMetrics(SSEConnectionMetrics(connectionReused: reused))
  }

  private func drainBuffer() {
    while let range = byteBuffer.range(of: Self.frameDelimiter) {
      let frameData = byteBuffer.subdata(in: byteBuffer.startIndex..<range.lowerBound)
      byteBuffer.removeSubrange(byteBuffer.startIndex..<range.upperBound)
      if let rawEvent = String(data: frameData, encoding: .utf8) {
        parseAndEmit(rawEvent)
      }
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
