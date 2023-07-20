from pathlib import Path

from google.cloud.bigquery import SchemaField
from odd_collector_sdk.grammar_parser.build_dataset_field import DatasetFieldBuilder
from odd_models import DataSetField, Type
from oddrn_generator import BigQueryStorageGenerator

from odd_collector_gcp.adapters.bigquery_storage.dto import BQField

BIG_QUERY_STORAGE_TYPE_MAPPING = {
    "STRING": Type.TYPE_STRING,
    "BYTES": Type.TYPE_BINARY,
    "INT64": Type.TYPE_INTEGER,
    "INTEGER": Type.TYPE_INTEGER,
    "FLOAT64": Type.TYPE_NUMBER,
    "FLOAT": Type.TYPE_NUMBER,
    "NUMERIC": Type.TYPE_NUMBER,
    "BIGNUMERIC": Type.TYPE_NUMBER,
    "BOOLEAN": Type.TYPE_BOOLEAN,
    "BOOL": Type.TYPE_BOOLEAN,
    "TIMESTAMP": Type.TYPE_DATETIME,
    "DATE": Type.TYPE_DATETIME,
    "TIME": Type.TYPE_TIME,
    "DATETIME": Type.TYPE_DATETIME,
    "GEOGRAPHY": Type.TYPE_UNKNOWN,  # We don't have a specific GEOGRAPHY type
    "ARRAY": Type.TYPE_LIST,  # Considering ARRAY as list
    "STRUCT": Type.TYPE_STRUCT,
    "RECORD": Type.TYPE_STRUCT,  # STRUCT and RECORD can be considered as same
}


def map_field(
    oddrn_generator: BigQueryStorageGenerator, column: BQField
) -> list[DataSetField]:
    field_builder = DatasetFieldBuilder(
        data_source="gcs_delta",
        oddrn_generator=oddrn_generator,
        parser_config_path=Path(
            "odd_collector_gcp/adapters/bigquery_storage/mappers/grammar/field_types.lark"
        ).absolute(),
        odd_types_map=BIG_QUERY_STORAGE_TYPE_MAPPING,
    )
    return field_builder.build_dataset_field(column)
