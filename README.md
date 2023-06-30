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
 - [BigTable](#bigtable)

### __BigQuery__
```yaml
type: bigquery_storage
name: bigquery_storage
project: <any_project_name>
```

### __BigTable__
```yaml
type: bigtable
name: bigtable
project: <any_project_name>
rows_limit: 10 # get combination of all types in table used across the first N rows.
```

### __GoogleCloudStorage__
```yaml
type: gcs
name: gcs_adapter
filename_filter: # Optional. Default filter allows each file to be ingested to platform.
  include: [ '.*.parquet' ]
  exclude: [ 'dev_.*' ]
datasets:
  # Recursive fetch for all objects in the bucket.
  - bucket: my_bucket
  # Explicitly specify the prefix to file.
  - bucket: my_bucket
    prefix: folder/subfolder/file.csv
  # When we want to use the folder as a dataset. Very useful for partitioned datasets.
  # I.e it can be Hive partitioned dataset with structure like this:
  # s3://my_bucket/partitioned_data/year=2019/month=01/...
  - bucket: my_bucket
    prefix: partitioned_data/
    folder_as_dataset:
      file_format: parquet
      flavor: hive

  #field_names must be provided if partition flavor was not used. I.e for structure like this:
  # s3://my_bucket/partitioned_data/year/...
  - bucket: my_bucket
    prefix: partitioned_data/
    folder_as_dataset:
      file_format: csv
      field_names: ['year']
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