//
//  HybridSSEClient.swift
//  nitro-sse-client
//

import Foundation
import NitroModules

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
    client?.handleResponse(task: dataTask, response: response)
    completionHandler(.allow)
  }

  func urlSession(_ session: URLSession, dataTask: URLSessionDataTask, didReceive data: Data) {
    client?.handleData(data, task: dataTask)
  }

  func urlSession(_ session: URLSession, task: URLSessionTask, didCompleteWithError error: Error?) {
    client?.handleCompletion(error: error, task: task)
  }

  func urlSession(_ session: URLSession, task: URLSessionTask, didFinishCollecting metrics: URLSessionTaskMetrics) {
    // Deliberately NOT gated by task identity — this is expected to fire for a task that a
    // newer connect() has already superseded (that's how a superseded connection's close
    // metrics get reported at all), unlike the callbacks above.
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

private let defaultReconnectIntervalMs: Double = 3000
private let defaultMaxReconnectIntervalMs: Double = 30000
private let defaultJitterFactor: Double = 0.5
// Error responses (4xx/5xx) are typically small JSON/HTML bodies — bounded so a misbehaving
// server streaming an enormous error page can't grow this unboundedly before completion.
private let maxErrorBodyBytes = 8192

class HybridSSEClient: HybridSSEClientSpec {
  var onMessage: (SSEMessageEvent) -> Void = { _ in }
  var onOpen: () -> Void = { }
  var onError: (SSEError) -> Void = { _ in }
  var onClose: () -> Void = { }
  var onMetrics: (SSEConnectionMetrics) -> Void = { _ in }
  var onStateChange: (SSEConnectionState) -> Void = { _ in }

  private let streamDelegate = SSEStreamDelegate()

  private var currentTask: URLSessionDataTask?
  // Raw bytes rather than a Swift String: String's range(of:)/+= are Unicode-grapheme-aware and
  // re-scan/re-copy the whole buffer on every call, too slow to do on every network chunk for a
  // chatty stream — this buffer is only ever decoded to a String once a full frame is isolated.
  private var byteBuffer = Data()
  private static let frameDelimiter = Data([0x0A, 0x0A]) // "\n\n"
  private var connectStartedAt: Date?
  private var firstByteLogged = false

  // Set in handleResponse when the response is being treated as an error (non-2xx status, or a
  // Content-Type mismatch); handleData then buffers the body here (instead of feeding it through
  // the normal SSE byteBuffer/parser) so it can be reported as the onError message once the
  // response completes.
  private var pendingErrorType: SSEErrorType?
  private var pendingErrorStatusCode: Int?
  private var errorBodyData = Data()

  // Reconnect state. connectURL/connectHeaders/connectMethod/connectBody/session are remembered
  // so an automatic reconnect can repeat the same connect() call; reconnectEnabled/maxAttempts/
  // retryOnClientError come from the caller's SSEReconnectOptions, resolved once per explicit
  // connect(). currentIntervalMs starts at options.intervalMs (default 3s) and is overridden
  // per-stream by a `retry:` field from the server; it resets back to the option's value on the
  // next *explicit* connect().
  private var connectURL: URL?
  private var connectHeaders: [String: String]?
  private var connectMethod: String?
  private var connectBody: String?
  private var connectSession: SSESessionOptions?
  private var shouldValidateContentType = true
  private var reconnectEnabled = true
  private var reconnectMaxAttempts: Double?
  private var retryOnClientError = false
  private var currentIntervalMs = defaultReconnectIntervalMs
  private var maxIntervalMs = defaultMaxReconnectIntervalMs
  private var jitterFactor = defaultJitterFactor
  private var reconnectAttempts = 0
  private var pendingReconnect: DispatchWorkItem?
  // Set by disconnect(), cleared at the start of every explicit connect() — distinguishes "the
  // caller asked us to stop" from a connection merely ending, which is what schedules a retry.
  private var intentionallyStopped = false
  private var lastEventId: String?
  private var currentState: SSEConnectionState = .idle

  public override init() {
    super.init()
    streamDelegate.client = self
  }

  func connect(
    url: String,
    headers: [String: String]?,
    session: SSESessionOptions?,
    reconnect: SSEReconnectOptions?,
    httpMethod: String?,
    body: String?,
    validateContentType: Bool?
  ) throws {
    // Validated before touching any existing connection, so a bad URL on a reconnect attempt
    // doesn't tear down a connection that was working fine — and reported through onError like
    // any other connection failure, rather than left silent or thrown as an uncaught JS exception.
    guard let nsUrl = URL(string: url) else {
      onError(SSEError(message: "Invalid URL: \(url)", type: .exception, statusCode: nil))
      return
    }

    pendingReconnect?.cancel()
    pendingReconnect = nil
    intentionallyStopped = false
    reconnectAttempts = 0
    lastEventId = nil

    connectURL = nsUrl
    connectHeaders = headers
    connectMethod = httpMethod
    connectBody = body
    connectSession = session
    shouldValidateContentType = validateContentType ?? true
    reconnectEnabled = reconnect?.enabled ?? true
    currentIntervalMs = reconnect?.intervalMs ?? defaultReconnectIntervalMs
    maxIntervalMs = reconnect?.maxIntervalMs ?? defaultMaxReconnectIntervalMs
    jitterFactor = reconnect?.jitterFactor ?? defaultJitterFactor
    reconnectMaxAttempts = reconnect?.maxAttempts
    retryOnClientError = reconnect?.retryOnClientError ?? false

    setState(.connecting)
    performConnect(isReconnect: false)
  }

  // Only fires onStateChange when the state actually changes — callers can transition through
  // the same state repeatedly (e.g. scheduleReconnectIfNeeded on every failed attempt) without
  // spamming duplicate events.
  private func setState(_ newState: SSEConnectionState) {
    guard currentState != newState else { return }
    currentState = newState
    onStateChange(newState)
  }

  private func performConnect(isReconnect: Bool) {
    guard let nsUrl = connectURL else { return }

    // Cancelling here (rather than tearing down the shared session) is what lets connection N+1
    // reuse the pool built up by connection N — only the task is torn down, never the session.
    currentTask?.cancel()

    byteBuffer.removeAll(keepingCapacity: false)
    pendingErrorType = nil
    pendingErrorStatusCode = nil
    errorBodyData.removeAll(keepingCapacity: false)
    firstByteLogged = false
    connectStartedAt = Date()

    var request = URLRequest(url: nsUrl)
    request.httpMethod = connectMethod ?? "GET"
    if let connectBody {
      request.httpBody = connectBody.data(using: .utf8)
    }
    request.setValue("text/event-stream", forHTTPHeaderField: "Accept")
    // Some SSE providers (e.g. Wikimedia) reject requests carrying the platform's generic
    // default User-Agent; identify this library instead of leaving it unset.
    request.setValue(
      "react-native-nitro-sse-client (+https://github.com/TaranVitalii/react-native-nitro-sse-client)",
      forHTTPHeaderField: "User-Agent"
    )
    // Applied after the defaults above, so a caller can override Accept/User-Agent too if they
    // need to — e.g. Authorization for a protected endpoint.
    if let connectHeaders {
      for (key, value) in connectHeaders {
        request.setValue(value, forHTTPHeaderField: key)
      }
    }
    // Only sent on an automatic reconnect that has actually seen an id: field — an explicit
    // connect() always starts a fresh logical session (see lastEventId reset in connect()).
    if isReconnect, let lastEventId {
      request.setValue(lastEventId, forHTTPHeaderField: "Last-Event-ID")
    }

    let sharedSession = SharedSession.get(connectSession)
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
    pendingReconnect?.cancel()
    pendingReconnect = nil
    intentionallyStopped = true
    let hadActiveTask = currentTask != nil
    currentTask?.cancel()
    currentTask = nil
    setState(.closed)
    if hadActiveTask {
      onClose()
    }
  }

  /// Called by Nitro when the JS side releases this object (GC), or explicitly via
  /// `nativeObject.dispose()` — without this, an abandoned SSEStream that never called
  /// disconnect()/destroy() would leave its task running against the shared session forever.
  /// A `HybridObject` protocol requirement (default no-op via extension), not a base-class
  /// method — so this is implemented directly, not with `override`.
  func dispose() {
    pendingReconnect?.cancel()
    currentTask?.cancel()
  }

  fileprivate func handleResponse(task: URLSessionTask, response: URLResponse) {
    // A superseding connect() may have already cancelled this task and started a new one before
    // this callback for the OLD task's response arrives — without this check, a late response
    // like this would fire onOpen() for a connection that's no longer the active one.
    guard task === currentTask else { return }

    guard let httpResponse = response as? HTTPURLResponse else {
      reconnectAttempts = 0
      setState(.open)
      onOpen()
      return
    }

    guard (200..<300).contains(httpResponse.statusCode) else {
      // Non-2xx: don't fire onOpen at all — handleData buffers the error body instead of treating
      // it as SSE frames, and handleCompletion reports it once the response finishes.
      pendingErrorType = .http
      pendingErrorStatusCode = httpResponse.statusCode
      return
    }

    if shouldValidateContentType {
      let contentType = httpResponse.value(forHTTPHeaderField: "Content-Type")?.lowercased() ?? ""
      guard contentType.hasPrefix("text/event-stream") else {
        // 2xx but not actually SSE (a login redirect page, a JSON error body, etc.) — same
        // "buffer the body, don't fire onOpen" treatment as a non-2xx status.
        pendingErrorType = .invalidContentType
        return
      }
    }

    reconnectAttempts = 0
    setState(.open)
    onOpen()
  }

  fileprivate func handleData(_ data: Data, task: URLSessionTask) {
    // Same race as handleResponse(): reject bytes from a task that's no longer current, so a
    // superseded connection's late-arriving data can't get appended into the new connection's
    // (already-reset) byteBuffer.
    guard task === currentTask else { return }

    if pendingErrorType != nil {
      let remaining = maxErrorBodyBytes - errorBodyData.count
      if remaining > 0 {
        errorBodyData.append(data.prefix(remaining))
      }
      return
    }

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

  fileprivate func handleCompletion(error: Error?, task: URLSessionTask) {
    // A genuine (non-cancellation) failure/close on a task a newer connect() has already
    // superseded shouldn't surface as "the current connection failed" — it isn't, anymore.
    // disconnect() fires onClose synchronously itself, so a cancellation reaching here is always
    // either that (already handled) or a superseded task (nothing to report).
    guard task === currentTask else { return }

    if let errorType = pendingErrorType {
      let statusCode = pendingErrorStatusCode
      let bodyString = String(data: errorBodyData, encoding: .utf8) ?? ""
      let message = !bodyString.isEmpty
        ? bodyString
        : (errorType == .invalidContentType ? "Response Content-Type was not text/event-stream" : "")
      pendingErrorType = nil
      pendingErrorStatusCode = nil
      errorBodyData.removeAll(keepingCapacity: false)
      NSLog("[Native SSE][iOS] %@ error: %@", errorType == .http ? "HTTP" : "content-type", statusCode.map(String.init) ?? "n/a")
      onError(SSEError(message: message, type: errorType, statusCode: statusCode.map(Double.init)))
      onClose()
      scheduleReconnectIfNeeded(afterHttpStatus: statusCode, wasContentTypeError: errorType == .invalidContentType)
      return
    }

    if let nsError = error as NSError? {
      if nsError.code == NSURLErrorCancelled { return }
      NSLog("[Native SSE][iOS] error: %@", error!.localizedDescription)
      let type: SSEErrorType = nsError.code == NSURLErrorTimedOut ? .timeout : .network
      onError(SSEError(message: nsError.localizedDescription, type: type, statusCode: nil))
      onClose()
      scheduleReconnectIfNeeded(afterHttpStatus: nil, wasContentTypeError: false)
      return
    }

    // No error, not an HTTP error, task is done: the server closed the stream normally.
    onClose()
    scheduleReconnectIfNeeded(afterHttpStatus: nil, wasContentTypeError: false)
  }

  // A 4xx status (other than 429, a rate-limit signal worth retrying) reflects something wrong
  // with the request/server config that retrying identically won't fix — same for a Content-Type
  // mismatch. Both are skipped by default; retryOnClientError opts back into the old
  // retry-everything behavior.
  private func isRetryableByDefault(httpStatus: Int?, wasContentTypeError: Bool) -> Bool {
    if retryOnClientError { return true }
    if wasContentTypeError { return false }
    guard let httpStatus else { return true }
    if httpStatus == 429 { return true }
    return !(400..<500).contains(httpStatus)
  }

  private func scheduleReconnectIfNeeded(afterHttpStatus statusCode: Int?, wasContentTypeError: Bool) {
    guard !intentionallyStopped else { return }
    guard reconnectEnabled else {
      setState(.closed)
      return
    }
    guard isRetryableByDefault(httpStatus: statusCode, wasContentTypeError: wasContentTypeError) else {
      setState(.failed)
      return
    }
    if let maxAttempts = reconnectMaxAttempts, Double(reconnectAttempts) >= maxAttempts {
      setState(.failed)
      return
    }

    let delayMs = nextReconnectDelayMs(attempt: reconnectAttempts)
    reconnectAttempts += 1
    setState(.reconnecting)

    let work = DispatchWorkItem { [weak self] in
      guard let self, !self.intentionallyStopped else { return }
      self.performConnect(isReconnect: true)
    }
    pendingReconnect = work
    DispatchQueue.global(qos: .utility).asyncAfter(deadline: .now() + delayMs / 1000, execute: work)
  }

  // Exponential backoff with jitter: delay doubles with each consecutive failed attempt, starting
  // from currentIntervalMs (the base interval, or the server's last `retry:` value) and capped at
  // maxIntervalMs, then randomized by jitterFactor to avoid many clients retrying in lockstep
  // after a shared outage. `attempt` is 0 for the first scheduled reconnect (so it starts at
  // exactly currentIntervalMs before jitter), 1 for the second (2x), 2 for the third (4x), etc.
  private func nextReconnectDelayMs(attempt: Int) -> Double {
    let exponential = min(currentIntervalMs * pow(2, Double(attempt)), maxIntervalMs)
    guard jitterFactor > 0 else { return exponential }
    let spread = exponential * jitterFactor
    let jittered = exponential - spread / 2 + Double.random(in: 0...spread)
    return max(0, min(jittered, maxIntervalMs))
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
      } else if line.hasPrefix("retry:") {
        if let ms = Double(String(line.dropFirst(6)).trimmingCharacters(in: .whitespaces)) {
          currentIntervalMs = ms
        }
      }
    }

    // An `id:` field (even empty) updates lastEventId for the *next* reconnect's Last-Event-ID
    // header — empty resets it to unset, matching the SSE spec.
    if let id {
      lastEventId = id.isEmpty ? nil : id
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
