from dataclasses import dataclass
from typing import Any

from google.cloud.bigquery import Dataset, SchemaField, Table


@dataclass
class BigQueryDataset:
    dataset: Dataset
    tables: list[Table]


class BQField:
    def __init__(self, field: SchemaField):
        self.field = field

    @property
    def odd_metadata(self) -> dict:
        return {
            "mode": self.field.mode,
            "description": self.field.description,
            "fields": self.field.fields,
            "policy_tags": self.field.policy_tags,
            "precision": self.field.precision,
            "scale": self.field.scale,
            "max_length": self.field.max_length,
        }

    @property
    def name(self) -> str:
        return self.field.name

    @property
    def type(self) -> Any:
        return self.field.field_type
