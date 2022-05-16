from typing import Literal, Union

import pydantic
from odd_collector_sdk.domain.plugin import Plugin
from typing_extensions import Annotated


class GcpPlugin(Plugin):
    project: str


class BigQueryStoragePlugin(GcpPlugin):
    type: Literal["bigquery_storage"]


AvailablePlugin = Annotated[
    Union[BigQueryStoragePlugin],
    pydantic.Field(discriminator="type"),
]
