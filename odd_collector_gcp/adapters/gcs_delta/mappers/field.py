from typing import Any, Union

from deltalake._internal import ArrayType, Field, MapType, PrimitiveType, StructType
from odd_collector_sdk.utils.metadata import extract_metadata, DefinitionType
from odd_models.models import DataSetField, DataSetFieldType, Type
from oddrn_generator import GCSGenerator

from ..logger import logger
from ..models.field import DField

DELTA_TO_ODD_TYPE_MAP: dict[str, Type] = {
    "float": Type.TYPE_NUMBER,
    "struct": Type.TYPE_STRUCT,
    "bigint": Type.TYPE_INTEGER,
    "binary": Type.TYPE_BINARY,
    "boolean": Type.TYPE_BOOLEAN,
    "date": Type.TYPE_DATETIME,
    "decimal": Type.TYPE_NUMBER,
    "double": Type.TYPE_NUMBER,
    "void": Type.TYPE_UNKNOWN,
    "smallint": Type.TYPE_INTEGER,
    "timestamp": Type.TYPE_TIME,
    "tinyint": Type.TYPE_INTEGER,
    "array": Type.TYPE_LIST,
    "map": Type.TYPE_MAP,
    "int": Type.TYPE_INTEGER,
    "string": Type.TYPE_STRING,
    "interval": Type.TYPE_DURATION,
}


def map_to_odd_type(delta_type: str) -> Type:
    return DELTA_TO_ODD_TYPE_MAP.get(delta_type, Type.TYPE_UNKNOWN)


# def unknown_field(generator: GCSGenerator, field: Field) -> DataSetField:
#     generator.set_oddrn_paths(columns=field.name)
#     return DataSetField(
#         oddrn=generator.get_oddrn_by_path("columns"),
#         name=field.name,
#         type=DataSetFieldType(
#             type=Type.TYPE_UNKNOWN,
#             logical_type=field.type,
#             is_nullable=field.nullable,
#         ),
#     )
#
#
# def map_primitive(generator: GCSGenerator, field: Field) -> DataSetField:
#     generator.set_oddrn_paths(columns=field.name)
#     return DataSetField(
#         oddrn=generator.get_oddrn_by_path("columns"),
#         name=field.name,
#         type=DataSetFieldType(
#             type=map_to_odd_type(field.type.type),
#             logical_type=field.type.type,
#             is_nullable=field.nullable,
#         ),
#     )
#
#
# def map_map(generator: GCSGenerator, field: Field) -> DataSetField:
#     logger.error("Map not implemented yet")
#
#
# def map_struct(generator: GCSGenerator, field: Field) -> DataSetField:
#     logger.error("Struct not implemented yet")
#
#
# def map_array(generator: GCSGenerator, field: Field) -> DataSetField:
#     logger.error("Array not implemented yet")
#
#
# def map_field(generator: GCSGenerator, field: Field) -> DataSetField:
#     type_ = field.type
#
#     if isinstance(type_, PrimitiveType):
#         return map_primitive(generator, field)
#     elif isinstance(type_, MapType):
#         return map_struct(generator, field)
#     elif isinstance(type_, StructType):
#         return map_map(generator, field)
#     elif isinstance(type_, ArrayType):
#         return map_array(generator, field)
#     else:
#         logger.error(f"Unknown field type: {field.type}")
#

def build_dataset_field(
    field: DField, oddrn_generator: GCSGenerator
) -> list[DataSetField]:
    logger.debug(f"Build dataset field for {field.name} with type {field.type}")
    type_ = field.type

    generated_dataset_fields = []

    def _build_ds_field_from_type(
        field_name: str,
        field_type: Union[PrimitiveType, MapType, ArrayType, StructType],
        parent_oddrn=None,
    ):

        if parent_oddrn is None:
            oddrn = oddrn_generator.get_oddrn_by_path("columns", field_name)
        else:
            oddrn = f"{parent_oddrn}/keys/{field_name}"

        if isinstance(field_type, StructType):
            generated_dataset_fields.append(
                DataSetField(
                    oddrn=oddrn,
                    name=field_name,
                    metadata=[
                        extract_metadata(
                            "delta_table", field, DefinitionType.DATASET_FIELD
                        )
                    ],
                    type=DataSetFieldType(
                        type=Type.TYPE_STRUCT,
                        logical_type=field_type.type,
                        is_nullable=False,
                    ),
                    owner=None,
                    parent_field_oddrn=parent_oddrn,
                )
            )
            for field_name, field_type in field_type.fields.items():
                _build_ds_field_from_type(field_name, field_type, oddrn)
        elif isinstance(field_type, MapType):
            generated_dataset_fields.append(
                DataSetField(
                    oddrn=oddrn,
                    name=field_name,
                    metadata=[
                        extract_metadata(
                            "delta_table", field, DefinitionType.DATASET_FIELD
                        )
                    ],
                    type=DataSetFieldType(
                        type=Type.TYPE_MAP,
                        logical_type=field_type.type,
                        is_nullable=False,
                    ),
                    owner=None,
                    parent_field_oddrn=parent_oddrn,
                )
            )
            _build_ds_field_from_type("Key", field_type.key_type, oddrn)
            _build_ds_field_from_type("Value", field_type.value_type, oddrn)

        else:
            odd_type = get_odd_type(field_type)
            logical_type = field_type.type
            logger.debug(
                f"Column {field_name} has ODD type {odd_type} and logical type {logical_type}"
            )
            generated_dataset_fields.append(
                DataSetField(
                    oddrn=oddrn,
                    name=field_name,
                    metadata=[
                        extract_metadata(
                            "delta_table", field, DefinitionType.DATASET_FIELD
                        )
                    ],
                    type=DataSetFieldType(
                        type=odd_type,
                        logical_type=logical_type,
                        is_nullable=False,
                    ),
                    owner=None,
                    parent_field_oddrn=parent_oddrn,
                )
            )

    _build_ds_field_from_type(field.name, type_)
    return generated_dataset_fields


# def get_logical_type(type_field: Any) -> str:
#     if isinstance(type_field, PrimitiveType):
#         return type_field.type
#     elif isinstance(type_field, ArrayType):
#         return f"Array({get_logical_type(type_field.type)})"
#     elif isinstance(type_field, MapType):
#         return f"Map({get_logical_type(type_field.key_type)}, {get_logical_type(type_field.value_type)})"
#     elif isinstance(type_field, str):
#         return type_field
#     elif isinstance(type_field, StructType):
#         return (
#             "Struct("
#             + ", ".join(
#                 f"{name}: {get_logical_type(type)}"
#                 for name, type in type_field.fields.items()
#             )
#             + ")"
#         )
#     else:
#         return "Unknown"


def get_odd_type(
    delta_type: Union[PrimitiveType, MapType, ArrayType, StructType]
) -> Type:
    return DELTA_TO_ODD_TYPE_MAP.get(delta_type.type, Type.TYPE_UNKNOWN)


def map_field(oddrn_generator: GCSGenerator, column: DField) -> list[DataSetField]:
    return build_dataset_field(column, oddrn_generator)
