from functools import reduce
from operator import iconcat

from google.cloud.bigquery import Table
from odd_collector_sdk.utils.metadata import DefinitionType, extract_metadata
from odd_models.models import (
    DataEntity,
    DataEntityGroup,
    DataEntityType,
    DataSet,
    DataSetField,
    DataSetFieldType,
    Type,
)
from oddrn_generator import BigQueryStorageGenerator

from odd_collector_gcp.adapters.bigquery_storage.dto import BigQueryDataset, BQField

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
    "GEOGRAPHY": Type.TYPE_UNKNOWN,
    "ARRAY": Type.TYPE_LIST,
    "STRUCT": Type.TYPE_STRUCT,
    "RECORD": Type.TYPE_STRUCT,
}


class BigQueryStorageMapper:
    def __init__(self, oddrn_generator: BigQueryStorageGenerator):
        self.oddrn_generator = oddrn_generator

    def map_datasets(self, datasets: list[BigQueryDataset]) -> list[DataEntity]:
        return reduce(iconcat, [self.map_dataset(d) for d in datasets], [])

    def map_dataset(self, dataset_dto: BigQueryDataset) -> list[DataEntity]:
        dataset = dataset_dto.dataset
        self.oddrn_generator.set_oddrn_paths(datasets=dataset.dataset_id)

        tables = [self.map_table(t) for t in dataset_dto.tables]

        database_service_deg = DataEntity(
            oddrn=self.oddrn_generator.get_oddrn_by_path("datasets"),
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
        self.oddrn_generator.set_oddrn_paths(tables=table.table_id)
        fields = [BQField(field) for field in table.schema]
        field_list = []
        for field in fields:
            processed_ds_fields = self.map_field(field)
            field_list.extend(processed_ds_fields)

        return DataEntity(
            oddrn=self.oddrn_generator.get_oddrn_by_path("tables"),
            name=table.table_id,
            description=table.description,
            metadata=[],
            created_at=table.created,
            updated_at=table.modified,
            type=DataEntityType.TABLE,
            dataset=DataSet(rows_number=table.num_rows, field_list=field_list),
        )

    def map_field(self, field: BQField) -> list[DataSetField]:
        if field.type == "RECORD":
            record_field = self.map_simple_field(field)
            return [
                self.map_simple_field(f, record_field.oddrn) for f in field.fields
            ] + [record_field]
        elif field.mode == "REPEATED":
            array_field = self.map_simple_field(field)
            element_field = self.map_simple_field(field, array_field.oddrn, True)
            return [array_field, element_field]
        return [self.map_simple_field(field)]

    def map_simple_field(
        self, field: BQField, parent_oddrn: str = None, array_element: bool = False
    ) -> DataSetField:
        field_type = (
            field.type if field.mode != "REPEATED" or array_element else "ARRAY"
        )
        field_name = "Element" if array_element else field.name
        if parent_oddrn:
            oddrn = f"{parent_oddrn}/keys/{field_name}"
        else:
            oddrn = self.oddrn_generator.get_oddrn_by_path("columns", field_name)

        return DataSetField(
            oddrn=oddrn,
            name=field_name,
            description=field.description,
            parent_field_oddrn=parent_oddrn,
            metadata=[
                extract_metadata("bigquery", field, DefinitionType.DATASET_FIELD)
            ],
            type=DataSetFieldType(
                type=BIG_QUERY_STORAGE_TYPE_MAPPING.get(field_type, Type.TYPE_UNKNOWN),
                logical_type=field_type,
                is_nullable=field.is_nullable,
            ),
        )
