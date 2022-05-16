from typing import Optional

from oddrn_generator import Generator
from oddrn_generator.path_models import BasePathsModel
from oddrn_generator.server_models import AbstractServerModel
from pydantic import BaseModel


class GCPCloudModel(AbstractServerModel, BaseModel):
    project: str

    def __str__(self) -> str:
        return f"cloud/gcp/project/{self.project}"


class BigQueryStoragePathsModel(BasePathsModel):
    datasets: Optional[str]
    tables: Optional[str]
    columns: Optional[str]

    class Config:
        dependencies_map = {
            "datasets": ("datasets",),
            "tables": ("datasets", "tables"),
            "columns": ("datasets", "tables", "columns"),
        }


class BigQueryStorageGenerator(Generator):
    source = "bigquery_storage"
    paths_model = BigQueryStoragePathsModel
    server_model = GCPCloudModel
