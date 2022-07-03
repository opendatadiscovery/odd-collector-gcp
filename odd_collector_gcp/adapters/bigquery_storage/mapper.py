from functools import reduce
from operator import iconcat
from typing import List

from google.cloud.bigquery import Table, SchemaField
from odd_models.models import (
    DataEntity,
    DataEntityType,
    DataEntityGroup,
    DataSet,
    DataSetField,
    DataSetFieldType,
    Type,
    DataSetFieldStat,
    StringFieldStat,
)

from odd_collector_gcp.adapters.bigquery_storage.dto import BigQueryDataset
from odd_collector_gcp.adapters.bigquery_storage.generator import (
    BigQueryStorageGenerator,
)

# Keys are lowercased so that it'd be easy to perform a case-insensitive lookup
_BIG_QUERY_STORAGE_TYPE_MAPPING = {
    "string": Type.TYPE_STRING,
    "bytes": Type.TYPE_BINARY,
    "integer": Type.TYPE_INTEGER,
    "float": Type.TYPE_NUMBER,
    "numeric": Type.TYPE_NUMBER,
    "bignumeric": Type.TYPE_NUMBER,
    "boolean": Type.TYPE_BOOLEAN,
    "timestamp": Type.TYPE_DATETIME,
    "date": Type.TYPE_DATETIME,
    "time": Type.TYPE_TIME,
    "datetime": Type.TYPE_DATETIME,
    "record": Type.TYPE_STRUCT,
}


class BigQueryStorageMapper:
    def __init__(self, oddrn_generator: BigQueryStorageGenerator):
        self.__oddrn_generator = oddrn_generator

    def map_datasets(self, datasets: List[BigQueryDataset]) -> List[DataEntity]:
        return reduce(iconcat, [self.map_dataset(d) for d in datasets], [])

    def map_dataset(self, dataset_dto: BigQueryDataset) -> List[DataEntity]:
        dataset = dataset_dto.dataset
        self.__oddrn_generator.set_oddrn_paths(datasets=dataset.dataset_id)

        tables = [self.map_table(t) for t in dataset_dto.tables]

        database_service_deg = DataEntity(
            oddrn=self.__oddrn_generator.get_oddrn_by_path("datasets"),
            name=dataset.dataset_id,
            description=dataset.description,
            metadata=[],
            created_at=dataset.created,
            updated_at=dataset.modified,
            type=DataEntityType.DATABASE_SERVICE,
            data_entity_group=DataEntityGroup(entities_list=[t.oddrn for t in tables]),
        )

        return tables + [database_service_deg]

    def map_table(self, table: Table) -> DataEntity:
        self.__oddrn_generator.set_oddrn_paths(tables=table.table_id)

        return DataEntity(
            oddrn=self.__oddrn_generator.get_oddrn_by_path("tables"),
            name=table.table_id,
            description=table.description,
            metadata=[],
            created_at=table.created,
            updated_at=table.modified,
            type=DataEntityType.TABLE,
            dataset=DataSet(
                rows_number=table.num_rows, field_list=self.map_schema(table.schema)
            ),
        )

    def map_schema(self, schema: SchemaField) -> List[DataSetField]:
        if isinstance(schema, list):
            return reduce(iconcat, [self.map_field(f) for f in schema], [])

        return reduce(iconcat, [self.map_field(f) for f in schema.fields], [])

    def map_field(self, field: SchemaField) -> List[DataSetField]:
        if field.field_type == "RECORD":
            record_field = self.map_simple_field(field)
            return [
                self.map_simple_field(f, record_field.oddrn) for f in field.fields
            ] + [record_field]

        return [self.map_simple_field(field)]

    def map_simple_field(
        self, field_schema: SchemaField, parent_oddrn: str = None
    ) -> DataSetField:
        field = DataSetField(
            oddrn=self.__oddrn_generator.get_oddrn_by_path(
                "columns", field_schema.name
            ),
            name=field_schema.name,
            description=field_schema.description,
            parent_field_oddrn=parent_oddrn,
            metadata=[],
            type=DataSetFieldType(
                type=_BIG_QUERY_STORAGE_TYPE_MAPPING.get(
                    field_schema.field_type.lower(), Type.TYPE_UNKNOWN
                ),
                logical_type=field_schema.field_type,
                is_nullable=field_schema.is_nullable,
            ),
        )

        return field
