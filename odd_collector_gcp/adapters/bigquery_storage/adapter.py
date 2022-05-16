from typing import List

from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntityList
from google.cloud import bigquery

from odd_collector_gcp.adapters.bigquery_storage.dto import BigQueryDataset
from odd_collector_gcp.adapters.bigquery_storage.generator import (
    BigQueryStorageGenerator,
)
from odd_collector_gcp.adapters.bigquery_storage.mapper import BigQueryStorageMapper
from odd_collector_gcp.domain.plugin import BigQueryStoragePlugin


class Adapter(AbstractAdapter):
    def __init__(self, config: BigQueryStoragePlugin):
        self.__client = bigquery.Client(project=config.project)
        self.__generator = BigQueryStorageGenerator(
            cloud_settings={"project": config.project},
        )
        self.__mapper = BigQueryStorageMapper(oddrn_generator=self.__generator)

    def get_data_source_oddrn(self) -> str:
        return self.__generator.get_data_source_oddrn()

    def get_data_entity_list(self) -> DataEntityList:
        entities = self.__mapper.map_datasets(self.__fetch_datasets())

        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(), items=entities
        )

    def __fetch_datasets(self) -> List[BigQueryDataset]:
        datasets = []
        for dr in self.__client.list_datasets():
            dataset = BigQueryDataset(
                dataset=self.__client.get_dataset(dr.dataset_id),
                tables=[
                    self.__client.get_table(t) for t in self.__client.list_tables(dr)
                ],
            )
            datasets.append(dataset)

        return datasets
