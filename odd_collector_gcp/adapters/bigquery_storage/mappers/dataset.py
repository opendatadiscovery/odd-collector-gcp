from functools import reduce
from operator import iconcat

from odd_models import DataEntity, DataEntityGroup, DataEntityType
from oddrn_generator import BigQueryStorageGenerator

from odd_collector_gcp.adapters.bigquery_storage.dto import BigQueryDataset
from odd_collector_gcp.adapters.bigquery_storage.mappers.table import map_table


def map_dataset(
    oddrn_generator: BigQueryStorageGenerator, dataset_dto: BigQueryDataset
) -> list[DataEntity]:
    dataset = dataset_dto.dataset
    oddrn_generator.set_oddrn_paths(datasets=dataset.dataset_id)

    tables = [map_table(oddrn_generator, t) for t in dataset_dto.tables]

    database_service_deg = DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("datasets"),
        name=dataset.dataset_id,
        description=dataset.description,
        metadata=[],
        created_at=dataset.created,
        updated_at=dataset.modified,
        type=DataEntityType.DATABASE_SERVICE,
        data_entity_group=DataEntityGroup(entities_list=[t.oddrn for t in tables]),
    )

    return tables + [database_service_deg]


def map_datasets(
    oddrn_generator: BigQueryStorageGenerator, datasets: list[BigQueryDataset]
) -> list[DataEntity]:
    return reduce(iconcat, [map_dataset(oddrn_generator, d) for d in datasets], [])
