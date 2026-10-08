# Pairamid RabbitMQ Client

Python client library for exchanging Pairamid simulation inputs and results
through RabbitMQ using Keycloak OAuth2 client credentials.

## Installation

```bash
pip install pairamid-rabbitmq-client
```

The package requires Python 3.10 or newer and a reachable RabbitMQ broker.

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

All three OAuth2 variables are required. Keep the client secret in a local
environment file and never commit it. The broker queues must be provisioned
before clients connect; the library does not declare queues.

## Basic usage

```python
from pairamid_rabbitmq_client import PlatformClient

client = PlatformClient(
	host="localhost",
	port=5672,
	client_id="platform-client",
	client_secret="secret",
	token_url="https://keycloak.example.com/realms/pairamid/protocol/openid-connect/token",
)

with client:
	client.publish_input(
		"partner-id",
		'{"geometry": "basic", "material_combination": "potez"}',
		"simulation-001",
	)
```

The simulation name is sent as AMQP metadata; the JSON body is not modified.
On the partner side, polling stores each valid input as a uniquely named JSON
file and acknowledges it only after it has been written successfully:

```python
from pairamid_rabbitmq_client import PartnerClient

client = PartnerClient("partner-id")
client.poll_forever(client.input_queue)
```

After a simulation completes, the partner application explicitly publishes its
result file:

```python
client.publish_result("/path/to/result-file")
```

`poll_forever()` opens and closes a broker connection for each polling cycle.
`publish_result()` opens a connection for the individual publish operation.
Both operations use the Keycloak service client configured in the environment.

## Building the package

This project uses `uv_build` as its PEP 517 build backend:

```bash
uv build
```

The command creates a wheel and source distribution in `dist/`. Validate them
before uploading to PyPI:

```bash
uvx twine check dist/*
```

## License

GNU Affero General Public License v3.0 only. See [LICENSE](LICENSE).
