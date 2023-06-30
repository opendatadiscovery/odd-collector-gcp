import traceback as tb
from typing import Iterable

from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntityList
from oddrn_generator.generators import GCSGenerator
from odd_collector_gcp.domain.plugin import GCSPlugin
from .file_system import FileSystem
from .logger import logger
from .mapper.bucket import map_bucket
from google.cloud import storage


class Adapter(AbstractAdapter):
    def __init__(self, config: GCSPlugin) -> None:
        self.config = config
        self.__client = storage.Client(project=config.project)
        self.__generator = GCSGenerator(
            google_cloud_settings={"project": config.project},
        )
        self.fs = FileSystem(config)

    def get_data_source_oddrn(self) -> str:
        return self.__generator.get_data_source_oddrn()

    def get_data_entity_list(self) -> Iterable[DataEntityList]:
        for dataset_config in self.config.datasets:
            try:
                bucket = self.fs.get_bucket(dataset_config)
                data_entities = map_bucket(bucket, self.__generator)

                yield DataEntityList(
                    data_source_oddrn=self.get_data_source_oddrn(),
                    items=list(data_entities),
                )
            except Exception as e:
                logger.error(
                    f"Error while processing bucket {dataset_config.bucket}: {e}."
                    " SKIPPING."
                )
                logger.debug(tb.format_exc())
                continue
