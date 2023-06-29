from typing import List

from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntityList
from google.cloud import bigtable
from oddrn_generator import BigTableGenerator

from odd_collector_gcp.adapters.bigtable.mapper import BigTableMapper
from odd_collector_gcp.adapters.bigtable.models import (
    BigTableInstance,
    BigTableTable,
    BigTableColumn,
)
from odd_collector_gcp.domain.plugin import BigTablePlugin


class Adapter(AbstractAdapter):
    def __init__(self, config: BigTablePlugin):
        self.__client = bigtable.Client(project=config.project, admin=True)
        self.__generator = BigTableGenerator(
            google_cloud_settings={"project": config.project}
        )
        self.__mapper = BigTableMapper(oddrn_generator=self.__generator)

    def get_data_source_oddrn(self) -> str:
        return self.__generator.get_data_source_oddrn()

    def get_data_entity_list(self) -> DataEntityList:
        entities = self.__mapper.map_instances(self.__get_instances())

        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(), items=entities
        )

    def __get_instances(self) -> List[BigTableInstance]:
        instances = []
        for instance in self.__client.list_instances()[0]:
            tables = []
            for table in instance.list_tables():
                merged_columns = {}
                columns = []
                for row in table.read_rows(limit=10):
                    merged_columns = merged_columns | row.to_dict()
                for col_name, col_val in merged_columns.items():
                    col_val = col_val[0].value if any(col_val) else None
                    columns.append(
                        BigTableColumn(name=col_name.decode(), value=col_val)
                    )
                tables.append(BigTableTable(table_id=table.table_id, columns=columns))
            instances.append(
                BigTableInstance(
                    instance_id=instance.instance_id,
                    tables=tables,
                )
            )
        return instances
