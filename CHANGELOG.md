# Changelog

## 0.0.4

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
