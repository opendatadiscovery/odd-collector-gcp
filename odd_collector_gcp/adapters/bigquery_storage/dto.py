from dataclasses import dataclass
from typing import Any

from google.cloud.bigquery import Dataset, SchemaField, Table
from odd_collector_sdk.utils.metadata import HasMetadata


class MetadataMixin:
    data_object: Any
    excluded_properties = []  # This can be overridden in each class

    @property
    def odd_metadata(self) -> dict:
        properties = {}
        for attr in dir(self.data_object):
            if attr not in self.excluded_properties and isinstance(
                getattr(self.data_object.__class__, attr, None), property
            ):
                properties[attr] = getattr(self.data_object, attr)

        return properties


@dataclass
class BigQueryDataset(MetadataMixin, HasMetadata):
    data_object: Dataset
    tables: list[Table]
    excluded_properties = [
        "reference",
        "description",
        "dataset_id",
        "created",
        "updated",
        "access_entries",
    ]


@dataclass
class BigQueryTable(MetadataMixin, HasMetadata):
    data_object: Table
    excluded_properties = [
        "reference",
        "schema",
        "num_rows",
        "description",
        "table_id",
        "created",
        "updated",
    ]


@dataclass
class BigQueryField(MetadataMixin, HasMetadata):
    data_object: SchemaField
    excluded_properties = [
        "name",
        "field_type",
        "fields",
        "description",
        "is_nullable",
    ]
