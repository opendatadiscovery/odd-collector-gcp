## ODD Google Cloud Storage adapter

ODD GCS adapter is used for extracting datasets info and metadata from Google Cloud Storage. This adapter is an implementation of pull model (see more https://github.com/opendatadiscovery/opendatadiscovery-specification/blob/main/specification/specification.md#discovery-models). By default application gather data from GCS every minute, put it inside local cache and then ready to give it away by /entities API.

This service based on Python Flask and Connexion frameworks with APScheduler.

### Data entities:
| Entity type | Entity source |
|:----------------:|:---------:|
|Dataset|Bucket, Folder(Subfolders), Columns|

Adapter uses apache arrow technology to obtain metadata.
Currently supported dataset formats are parquet, csv(plain and gziped), tsv(plain and gziped)
Please note:

- dataset should contain files in same format, in case of format incompatibilities or corrupted files 
warning will be thrown with message like 
```
[2022-02-18 11:05:52,615] WARNING in gcs_schema_retriever: unable to parse dataset in odd-gcs-adapter/ with csv format
```
- Datasets with gziped files will take much longer to retrieve info because of compression 

- Adapter uses file extensions to identify format so make sure you are using .parquet, .csv, .csv.gz, .tsv, .tsv.gz extensions in files names


For more information about data entities see https://github.com/opendatadiscovery/opendatadiscovery-specification/blob/main/specification/specification.md#data-model-specification

## Quickstart
Application is ready to run out of the box by the docker-compose (see more https://docs.docker.com/compose/).

For more info about variables have a look at .env file in docker directory.

After docker-compose run successful, application is ready to accept connection on port :8080. 


#### Config for Helm:
```
podSecurityContext:
  fsGroup: 65534
image:
  pullPolicy: Always
  repository: 436866023604.dkr.ecr.eu-central-1.amazonaws.com/odd-s3-adapter
  tag: ci-655380
nameOverride: odd-gcs-adapter
labels:
  adapter: odd-gcs-adapter
config:
  envFrom:
  - configMapRef:
      name: odd-gcs-adapter
  env:
  - name: DEMO_GREETING
    value: "Hello from the environment"
  - name: DEMO_FAREWELL
    value: "Such a sweet sorrow"
```
More info about Helm config in https://github.com/opendatadiscovery/charts


## Requirements
- Python 3.9+
- google-cloud-storage 2.10.0+
