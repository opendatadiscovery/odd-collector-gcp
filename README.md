[![forthebadge](https://forthebadge.com/images/badges/built-with-love.svg)](https://forthebadge.com)
[![forthebadge](https://forthebadge.com/images/badges/for-you.svg)](https://forthebadge.com)
# odd-collector-gcp
ODD Collector GCP is a lightweight service which gathers metadata from all your Google Cloud Platform data sources.

To learn more about collector types and ODD Platform's architecture, [read the documentation](https://docs.opendatadiscovery.org/architecture).

## Preview
 - [Implemented adapters](#implemented-adapters)
 - [How to build](#how-to-build)
 - [Config example](#config-example)

## Implemented adapters
 - [BigQuery](#bigquery)

### __BigQuery__
```yaml
type: bigquery_storage
name: bigquery_storage
project: <any_project_name>
```

## How to build
```bash
docker build .
```

## Config example
Due to the Plugin is inherited from `pydantic.BaseSetting`, each field missed in `collector-config.yaml` can be taken from env variables.

Custom `.env` file for docker-compose.yaml
```
GOOGLE_APPLICATION_CREDENTIALS=
PLATFORM_HOST_URL=
```

Custom `collector-config.yaml`
```yaml
default_pulling_interval: 10
token: "<CREATED_COLLECTOR_TOKEN>"
platform_host_url: "http://localhost:8080"
plugins:
  - type: bigquery_storage
    name: bigquery_storage
    project: opendatadiscovery
```

docker-compose.yaml
```yaml
version: "3.8"
services:
  # --- ODD Platform ---
  database:
    ...
  odd-platform:
    ...
  odd-collector-aws:
    image: 'image_name'
    restart: always
    volumes:
      - collector_config.yaml:/app/collector_config.yaml
    environment:
      - GOOGLE_APPLICATION_CREDENTIALS=${GOOGLE_APPLICATION_CREDENTIALS}
      - PLATFORM_HOST_URL=${PLATFORM_HOST_URL}
    depends_on:
      - odd-platform
```