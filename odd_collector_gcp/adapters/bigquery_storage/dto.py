from dataclasses import dataclass

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
            "policy_tags": self.field.policy_tags,
            "precision": self.field.precision,
            "scale": self.field.scale,
            "max_length": self.field.max_length,
        }

    @property
    def name(self) -> str:
        return self.field.name

    @property
    def type(self) -> str:
        return self.field.field_type

    @property
    def fields(self) -> list:
        return [BQField(field) for field in self.field.fields]

    @property
    def description(self) -> str:
        return self.field.description

    @property
    def is_nullable(self) -> bool:
        return self.field.is_nullable

    @property
    def mode(self) -> str:
        return self.field.mode
