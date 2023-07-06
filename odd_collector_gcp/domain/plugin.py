from typing import Literal, Optional

from odd_collector_sdk.domain.plugin import Plugin
from odd_collector_sdk.domain.filter import Filter
from odd_collector_sdk.types import PluginFactory

from odd_collector_gcp.adapters.gcs.domain.parameters import GCSAdapterParams
from odd_collector_gcp.domain.dataset_config import DatasetConfig


class GcpPlugin(Plugin):
    project: str


class BigQueryStoragePlugin(GcpPlugin):
    type: Literal["bigquery_storage"]


class BigTablePlugin(GcpPlugin):
    type: Literal["bigtable"]
    rows_limit: Optional[int] = 10


class GCSPlugin(GcpPlugin):
    type: Literal["gcs"]
    datasets: list[DatasetConfig]
    parameters: Optional[GCSAdapterParams] = None
    filename_filter: Optional[Filter] = Filter()


PLUGIN_FACTORY: PluginFactory = {
    "bigquery_storage": BigQueryStoragePlugin,
    "bigtable": BigTablePlugin,
    "gcs": GCSPlugin,
}
