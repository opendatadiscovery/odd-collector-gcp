import dataclasses
from typing import List

from google.cloud.bigquery import Dataset, Table


@dataclasses.dataclass
class BigQueryDataset:
    dataset: Dataset
    tables: List[Table]
