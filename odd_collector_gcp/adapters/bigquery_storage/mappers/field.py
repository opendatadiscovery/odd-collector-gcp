from odd_models import DataSetField, DataSetFieldType, Type
from oddrn_generator import BigQueryStorageGenerator

from odd_collector_gcp.adapters.bigquery_storage.dto import BQField
from odd_collector_gcp.logger import logger

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


# def map_field(
#     oddrn_generator: BigQueryStorageGenerator, column: BQField
# ) -> list[DataSetField]:
#     field_builder = DatasetFieldBuilder(
#         data_source="gcs_delta",
#         oddrn_generator=oddrn_generator,
#         parser_config_path=Path(
#             "odd_collector_gcp/adapters/bigquery_storage/mappers/grammar/field_types.lark"
#         ).absolute(),
#         odd_types_map=BIG_QUERY_STORAGE_TYPE_MAPPING,
#     )
#     return field_builder.build_dataset_field(column)


class FieldMapper:
    def __init__(self, oddrn_generator: BigQueryStorageGenerator):
        self.oddrn_generator = oddrn_generator

    def map_field(self, field: BQField) -> list[DataSetField]:
        if field.type == "RECORD":
            record_field = self.map_simple_field(field)
            logger.info(f"PARENT ODDRN: {record_field.oddrn}")
            return [
                self.map_simple_field(f, record_field.oddrn) for f in field.fields
            ] + [record_field]

        return [self.map_simple_field(field)]

    def map_simple_field(
        self,
        field: BQField,
        parent_oddrn: str = None,
    ) -> DataSetField:
        if parent_oddrn:
            oddrn = f"{parent_oddrn}/keys/{field.name}"
            logger.info(type(oddrn))
        else:
            oddrn = self.oddrn_generator.get_oddrn_by_path("columns", field.name)
        entity = DataSetField(
            oddrn=oddrn,
            name=field.name,
            description=field.description,
            parent_field_oddrn=parent_oddrn,
            metadata=[],
            type=DataSetFieldType(
                type=BIG_QUERY_STORAGE_TYPE_MAPPING.get(field.type, Type.TYPE_UNKNOWN),
                logical_type=field.type,
                is_nullable=field.is_nullable,
            ),
        )

        return entity
