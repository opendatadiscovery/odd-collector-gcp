from typing import Literal, Union, Optional

import pydantic
from odd_collector_sdk.domain.plugin import Plugin
from odd_collector_sdk.domain.filter import Filter
from odd_collector_sdk.types import PluginFactory
from typing_extensions import Annotated

from odd_collector_gcp.domain.dataset_config import DatasetConfig


class GcpPlugin(Plugin):
    project: str


class BigQueryStoragePlugin(GcpPlugin):
    type: Literal["bigquery_storage"]


class GCSPlugin(GcpPlugin):
    type: Literal["gcs"]
    datasets: list[DatasetConfig]
    filename_filter: Optional[Filter] = Filter()


PLUGIN_FACTORY: PluginFactory = {
    "bigquery_storage": BigQueryStoragePlugin,
    "gcs": GCSPlugin,
}
