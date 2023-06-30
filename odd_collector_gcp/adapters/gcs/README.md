## ODD S3 adapter

ODD S3 adapter is used for extracting datasets info and metadata from S3 and S3 compatible object storages(MinIO). This adapter is implemetation of pull model (see more https://github.com/opendatadiscovery/opendatadiscovery-specification/blob/main/specification/specification.md#discovery-models). By default application gather data from S3 every minute, put it inside local cache and then ready to give it away by /entities API.

This service based on Python Flask and Connexion frameworks with APScheduler.

### Data entities:
| Entity type | Entity source |
|:----------------:|:---------:|
|Dataset|Bucket, Folder(Subfolders), Columns|

Adapter uses apache arrow technology to obtain metadata.
Currently supported dataset formats are parquet, csv(plain and gziped), tsv(plain and gziped)
Please note:

- dataset should contain files in same format, in case of format incompatibilities or currapted files 
warning will be trown with message like 
```
[2022-02-18 11:05:52,615] WARNING in s3_schema_retriever: unable to pars dataset in odd-s3-adapter/ with csv format
```
- Datasets with gziped files will take much longer to retrieve info because of compression 

- Adapter uses file extensions to identify format so make sure you are using .parquet, .csv, .csv.gz, .tsv, .tsv.gz extensions in files names


For more information about data entities see https://github.com/opendatadiscovery/opendatadiscovery-specification/blob/main/specification/specification.md#data-model-specification

## Quickstart
Application is ready to run out of the box by the docker-compose (see more https://docs.docker.com/compose/).
Strongly recommended to override next variables in docker-compose .env file:

```
AWS_REGION=
AWS_ACCESS_KEY_ID=   
AWS_SECRET_ACCESS_KEY=
S3_PATHS=
```

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
nameOverride: odd-s3-adapter
labels:
  adapter: odd-s3-adapter
config:
  envFrom:
  - configMapRef:
      name: odd-s3-adapter
  env:
  - name: DEMO_GREETING
    value: "Hello from the environment"
  - name: DEMO_FAREWELL
    value: "Such a sweet sorrow"
```
More info about Helm config in https://github.com/opendatadiscovery/charts


## Requirements
- Python 3.8
- S3 13+
