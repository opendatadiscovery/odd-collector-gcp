from typing import Literal

from odd_collector_sdk.domain.plugin import Plugin
from odd_collector_sdk.types import PluginFactory


class GcpPlugin(Plugin):
    project: str


class BigQueryStoragePlugin(GcpPlugin):
    type: Literal["bigquery_storage"]


class BigTablePlugin(GcpPlugin):
    type: Literal["bigtable"]


PLUGIN_FACTORY: PluginFactory = {
    "bigtable": BigTablePlugin,
    "bigquery_storage": BigQueryStoragePlugin,
}
