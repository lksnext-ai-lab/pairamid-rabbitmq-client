# Pairamid RabbitMQ Client

Python client library for exchanging Pairamid simulation inputs and results
through RabbitMQ using Keycloak OAuth2 client credentials.

## Installation

```bash
pip install "https://github.com/lksnext-ai-lab/pairamid-rabbitmq-client/releases/download/0.0.4/pairamid_rabbitmq_client-0.0.4-py3-none-any.whl"
```

The package requires Python 3.10 or newer and a reachable RabbitMQ broker.
The URL becomes available after the maintainer publishes release **0.0.4**.

## Configuration

The clients read connection settings from environment variables when values are
not passed explicitly:

| Variable | Example | Purpose |
| --- | --- | --- |
| `RABBITMQ_HOST` | `rabbitmq` | Broker hostname |
| `RABBITMQ_PORT` | `5672` | Broker port |
| `RABBITMQ_DEFAULT_VHOST` | `/` | Virtual host |
| `RABBITMQ_CLIENT_ID` | `platform-client` | OAuth2 client ID |
| `RABBITMQ_CLIENT_SECRET` | `secret` | OAuth2 client secret |
| `OAUTH2_TOKEN_URL` | `https://keycloak.example.com/realms/pairamid/protocol/openid-connect/token` | OAuth2 token endpoint |
| `SIMULATION_INPUT_DIR` | `./data` | Directory where input JSON files are stored |
| `CONSUMER_POLL_INTERVAL_SECONDS` | `30` | Delay between input queue polls |
| `RABBITMQ_EXECUTION_EXCHANGE` | `simulation_execution_exchange` | Platform execution exchange |
| `RABBITMQ_EVENTS_EXCHANGE` | `simulation_events_exchange` | Partner event exchange |

All three OAuth2 variables are required. Keep the client secret in a local
environment file and never commit it. The broker queues must be provisioned
before clients connect; the library does not declare queues.

## Version 0.0.4: confirmed execution and event publication

```python
import json
from pairamid_rabbitmq_client import PlatformClient

with open("execution-message.json", encoding="utf-8") as source:
    execution = json.load(source)

# Configure the platform service account through environment variables.
with PlatformClient() as client:
    client.publish_execution(
        "irtjv", execution["simulation_key"], execution, execution["message_id"]
    )
```

`execution-message.json` must follow the bundled execution contract. See the
[official example](tests/examples/simulation-execution-message.json). The body
is a mapping, not an already serialized string; the library validates it before
publication. The broker routes it with `<partner_id>.<simulation_key>`.
`partner_id` must match `^[a-z][a-z0-9_-]{0,99}$`: dots and wildcards are forbidden.
Simulation keys follow `^[a-z][a-z0-9_.-]{0,99}$` and may contain dots.

```python
from pairamid_rabbitmq_client import PartnerClient
from pairamid_rabbitmq_client.contracts import (
    build_input_downloaded_event, build_result_payload, build_succeeded_event,
)

downloaded = build_input_downloaded_event(execution)
partner_output = {"status": "completed", "value": 42}
payload = build_result_payload(execution, partner_output)
succeeded = build_succeeded_event(payload)

# Use the partner service account, not the platform account.
with PartnerClient("irtjv") as client:
    client.publish_event(downloaded["event_type"], downloaded, downloaded["event_id"])
    client.publish_event(succeeded["event_type"], succeeded, succeeded["event_id"])
```

The builders do not download inputs, run simulations, or watch files. Emit
`input_downloaded` only after input files have actually been stored. Those
adapter operations are a later release. The partner application will produce
`results-<simulation_key>-<simulation_execution_id>.json` containing **only its
output**; `build_result_payload(execution, output)` copies the original context
and preserves that output intact under `output`. `build_succeeded_event(payload)`
deep-copies the complete payload without transformations.

`build_failed_event(error_payload)` builds the terminal failure event, preserving
the error payload's `finished_at`. A terminal error must have `retryable=false`.
Builders generate UUID event IDs and UTC timestamps when applicable, and accept
explicit event IDs for durable retries. Persist/reuse the same event and ID when
retrying; rebuilding it generates a different ID.

### Contracts and validation

The five authoritative JSON Schemas are versioned copies from iTool, bundled
inside the wheel. `jsonschema` Draft 2020-12 and an offline `$id` registry validate
UUIDs, date-times, allowed fields, versions, and event success/failure rules.
No schema is downloaded at runtime.

```python
from pairamid_rabbitmq_client import ContractValidationError
from pairamid_rabbitmq_client.contracts import validate_execution_message

try:
    validate_execution_message(execution)
except ContractValidationError as error:
    for issue in error.errors:
        print(issue.path, issue.message)
    raise
```

Other validators: `validate_input_downloaded_event`, `validate_result_event`,
`validate_result_payload`, `validate_error_payload`. They return `None` on success
or raise `ContractValidationError`. Successful event context must also match its
nested result payload. Input/output contents remain opaque JSON: the library
does **not** validate simulation-specific parameters or physical results.
Unknown `schema_version`, non-JSON values and NaN/infinity are rejected.

### Low-level publication and identity

```python
from pairamid_rabbitmq_client import Publisher

with Publisher() as publisher:
    publisher.publish(
        "my_exchange", "my.routing.key", b'{"value":42}',
        message_id="stable-id", correlation_id="execution-id",
        headers={"trace": "example"}, timestamp=1792666800,
    )
```

This low-level API does not validate a messaging contract; high-level clients
do. Exchanges/queues must already exist. `execution_exchange` and
`events_exchange` constructor arguments override environment defaults; an empty
exchange string explicitly selects the default exchange.

Every channel enables confirms once; every publication uses `mandatory=True`,
`delivery_mode=2` and `content_type="application/json"`. High-level methods set
the message/event ID, execution correlation ID and Unix timestamp.

By default, `user_id` is read from the token's `preferred_username`, then
`client_id`, matching iTool's broker configuration. This local decoding **does
not verify JWT signatures**: RabbitMQ verifies authentication and checks the
published `user_id`. Explicit `Publisher(user_id="broker-identity")` supports a
different broker identity policy or opaque token. `set_user_id=False` opts out;
the consumer must not assume missing `user_id` is authenticated partner identity.
An opaque/malformed token without explicit identity raises an error by default.
Never pass tokens or secrets in message headers. No hot token refresh is added:
the provider obtains a current token when a connection is opened.

Errors preserve the original cause:

- `UnroutableMessageError`: mandatory publication had no destination.
- `PublishNackedError`: broker nack.
- `PublishConnectionError`: connection failure/loss; delivery may be ambiguous,
  so a retry can duplicate a message.
- Broker channel errors such as `403 ACCESS_REFUSED` or `406` identity mismatch
  stay explicit Pika exceptions, not transient/success-shaped fallbacks.

### Migration from 0.0.3

`publish(queue=..., message=..., headers=...)`, old positional queue publication,
`PlatformClient.publish_input()` and `PartnerClient.publish_result()` remain
available with `DeprecationWarning`. They still use the old default-exchange/
queue topology; **iTool has removed those scopes**, so retaining the Python API
does not restore access in its current environment. Set `results_queue=` to
override the legacy result queue if needed.

The legacy consumer and `input_queue` default are unchanged. It still uses
polling, unique file suffixes and the `simulation_name` header, and does not emit
`input_downloaded`. `execution_queue("lri")` returns the new queue name, but this
does not upgrade the consumer's materialization/ack behavior. Do not feed new
execution envelopes into the legacy file consumer as a substitute for the
future adapter. Legacy simulation filenames retain their old uppercase support;
the new contract uses lowercase keys.

## Development and contract synchronization

```bash
# Dependencies only; do not install this project as editable or from a local wheel.
uv sync --frozen --no-install-project --extra dev
PYTHONPATH=src uv run --no-sync python -m unittest discover -s tests -v
uv run --no-sync ruff check src tests scripts
uv run --no-sync mypy src
python scripts/sync_contracts.py --source ../pairamid-itool/docs/specs --check
```

To update snapshots deliberately, run the last command without `--check`.
It copies the five schemas and six official messaging examples and records
their hashes. CI checks snapshot integrity and validates all bundled examples.
Comparing against iTool is an explicit local release check with `--source`;
CI does not connect GitHub to the private iTool GitLab repository. Automatic
cross-repository drift monitoring is intentionally deferred. Run the local
comparison whenever messaging contracts change and before publishing a release.

CI tests Python 3.10–3.14. Opt-in live AMQP tests use the provisioned iTool topology,
idle queues and `RABBITMQ_INTEGRATION=1`, `OAUTH2_TOKEN_URL`,
`API_RABBITMQ_CLIENT_SECRET`, `PARTNER_RABBITMQ_CLIENT_SECRET`.
Set `SSL_CERT_FILE` to a trusted local CA for HTTPS Keycloak in development.
Never run integration consumption against queues containing real work.

## Building the package

This project uses `uv_build` as its PEP 517 build backend:

```bash
uv build
```

The command creates a wheel and source distribution in `dist/`. Validate them
before publishing:

```bash
uvx twine check dist/*
```

Publish manually by pushing the exact `0.0.4` tag on a commit already in `main`.
The release workflow verifies the version, runs tests, and uploads wheel/sdist
to GitHub Releases. iTool must keep using its existing published pin until
0.0.4 is actually published and checked; building here does not install it in iTool.

## License

GNU Affero General Public License v3.0 only. See [LICENSE](LICENSE).
