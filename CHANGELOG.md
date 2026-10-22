# Changelog

## 0.0.5

- **Breaking:** adopt messaging contracts 2.0 for independent simulations.
  Remove and reject `workflow_instance_name`; replace UUID execution IDs with
  positive database-assigned integer IDs (maximum `9007199254740991`).
- Keep execution IDs numeric in JSON and serialize them as decimal strings in
  AMQP `correlation_id`, including all partner event types.
- Update offline schemas, context checks, builders, examples and contract sync
  sources to the authoritative versioned v2 paths in iTool.
- Preserve UUID message/event/run/attempt IDs and exact partner output; reject
  v1 envelopes rather than silently converting them.
- Add numeric-ID boundaries, strict ID types, nested-context and removed-field
  regressions. Keep CRLF/LF synchronization checks and legacy API regressions.
- Document future partner-qualified filenames. Input materialization and
  result watchers are still not implemented.

## 0.0.4

- Normalize CRLF/LF when synchronizing and hashing contracts so integrity checks
  remain consistent between Windows worktrees and Linux CI checkouts.
- Add confirmed exchange/routing-key publication with complete AMQP properties,
  authenticated `user_id`, and distinct unroutable/nack/connection errors.
- Add validated `publish_execution` and `publish_event` methods and configurable
  execution/event exchanges.
- Bundle five offline JSON Schemas, validators, event builders and exact output
  preservation; add contract synchronization tooling and official example tests.
- Add unit tests, opt-in live AMQP tests, Python 3.10–3.14 CI and release checks.
- Deprecate queue/message publication, `publish_input` and `publish_result`,
  preserving their old queue behavior. Removed broker permissions are not
  restored by Python API compatibility.
- Require lowercase new simulation keys and single-segment partner IDs.
  Legacy simulation filenames retain uppercase support.
- Keep the legacy polling/file consumer unchanged. New input materialization,
  automatic downloaded events, file watchers and application identity checks
  are intentionally deferred to later releases.
