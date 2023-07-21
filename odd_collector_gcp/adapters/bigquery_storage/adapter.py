from google.cloud import bigquery
from odd_collector_sdk.domain.adapter import BaseAdapter
from odd_models.models import DataEntityList
from oddrn_generator.generators import BigQueryStorageGenerator

from odd_collector_gcp.adapters.bigquery_storage.dto import BigQueryDataset
from odd_collector_gcp.adapters.bigquery_storage.mapper import BigQueryStorageMapper
from odd_collector_gcp.domain.plugin import BigQueryStoragePlugin


class Adapter(BaseAdapter):
    config: BigQueryStoragePlugin
    generator: BigQueryStorageGenerator

    def __init__(self, config: BigQueryStoragePlugin):
        super().__init__(config)
        self.client = bigquery.Client(project=config.project)
        self.mapper = BigQueryStorageMapper(oddrn_generator=self.generator)

    def create_generator(self) -> BigQueryStorageGenerator:
        return BigQueryStorageGenerator(
            google_cloud_settings={"project": self.config.project},
        )

    def get_data_source_oddrn(self) -> str:
        return self.generator.get_data_source_oddrn()

    def get_data_entity_list(self) -> DataEntityList:
        entities = self.mapper.map_datasets(self.__fetch_datasets())
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(), items=entities
        )

    def __fetch_datasets(self) -> list[BigQueryDataset]:
        datasets = []
        for dr in self.client.list_datasets():
            dataset = BigQueryDataset(
                dataset=self.client.get_dataset(dr.dataset_id),
                tables=[self.client.get_table(t) for t in self.client.list_tables(dr)],
            )
            datasets.append(dataset)

        return datasets
