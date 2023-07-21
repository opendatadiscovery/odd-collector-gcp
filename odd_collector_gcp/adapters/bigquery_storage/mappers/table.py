from functools import reduce
from operator import iconcat

from google.cloud.bigquery import SchemaField, Table
from odd_models import DataEntity, DataEntityType, DataSet, DataSetField
from oddrn_generator import BigQueryStorageGenerator

from odd_collector_gcp.adapters.bigquery_storage.dto import BQField
from odd_collector_gcp.adapters.bigquery_storage.mappers.field import FieldMapper


def map_table(oddrn_generator: BigQueryStorageGenerator, table: Table) -> DataEntity:
    oddrn_generator.set_oddrn_paths(tables=table.table_id)
    mapper = FieldMapper(oddrn_generator)
    fields = [BQField(field) for field in table.schema]
    field_list = []
    for field in fields:
        processed_ds_fields = mapper.map_field(field)
        field_list.extend(processed_ds_fields)

    return DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("tables"),
        name=table.table_id,
        description=table.description,
        metadata=[],
        created_at=table.created,
        updated_at=table.modified,
        type=DataEntityType.TABLE,
        dataset=DataSet(rows_number=table.num_rows, field_list=field_list),
    )


# def map_schema(schema: SchemaField) -> list[DataSetField]:
#     if isinstance(schema, list):
#         return reduce(iconcat, [map_field(f) for f in schema], [])
#
#     return reduce(iconcat, [map_field(f) for f in schema.fields], [])
