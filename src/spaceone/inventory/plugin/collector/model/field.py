from typing import Literal, Union
from pydantic import BaseModel

__all__ = [
    "TextField",
    "DictField",
    "SizeField",
    "ProgressField",
    "DatetimeField",
    "StateField",
    "BadgeField",
    "ImageField",
    "MoreField",
    "EnumField",
    "EnumBadgeField",
    "EnumStateField",
    "EnumDatetimeField",
    "EnumImageField",
]

BACKGROUND_COLORS = (
    "black",
    "white",
    "gray",
    "gray.100",
    "gray.200",
    "gray.300",
    "gray.400",
    "gray.500",
    "gray.600",
    "gray.700",
    "gray.800",
    "gray.900",
    "red",
    "red.100",
    "red.200",
    "red.300",
    "red.400",
    "red.500",
    "red.600",
    "red.700",
    "red.800",
    "red.900",
    "coral",
    "coral.100",
    "coral.200",
    "coral.300",
    "coral.400",
    "coral.500",
    "coral.600",
    "coral.700",
    "coral.800",
    "coral.900",
    "yellow",
    "yellow.100",
    "yellow.200",
    "yellow.300",
    "yellow.400",
    "yellow.500",
    "yellow.600",
    "yellow.700",
    "yellow.800",
    "yellow.900",
    "green",
    "green.100",
    "green.200",
    "green.300",
    "green.400",
    "green.500",
    "green.600",
    "green.700",
    "green.800",
    "green.900",
    "blue",
    "blue.100",
    "blue.200",
    "blue.300",
    "blue.400",
    "blue.500",
    "blue.600",
    "blue.700",
    "blue.800",
    "blue.900",
    "violet",
    "violet.100",
    "violet.200",
    "violet.300",
    "violet.400",
    "violet.500",
    "violet.600",
    "violet.700",
    "violet.800",
    "violet.900",
    "peacock",
    "peacock.100",
    "peacock.200",
    "peacock.300",
    "peacock.400",
    "peacock.500",
    "peacock.600",
    "peacock.700",
    "peacock.800",
    "peacock.900",
    "indigo",
    "indigo.100",
    "indigo.200",
    "indigo.300",
    "indigo.400",
    "indigo.500",
    "indigo.600",
    "indigo.700",
    "indigo.800",
    "indigo.900",
)


class SizeOptions(BaseModel):
    display_unit: Literal["BYTES", "KB", "MB", "GB", "TB", "PB"] = None
    source_unit: Literal["BYTES", "KB", "MB", "GB", "TB", "PB"] = None
    is_optional: bool = None


class ProgressOptions(BaseModel):
    unit: str = None
    is_optional: bool = None


class DatetimeOptions(BaseModel):
    source_type: Literal["iso8601", "timestamp"] = None
    source_format: str = None
    display_format: str = None
    is_optional: bool = None


class StateIconOptions(BaseModel):
    image: str = None
    color: Literal[BACKGROUND_COLORS] = None


class StateOptions(BaseModel):
    text_color: str = None
    icon: StateIconOptions = None
    is_optional: bool = None


class BadgeOptions(BaseModel):
    text_color: str = None
    shape: Literal["SQUARE", "ROUND"] = None
    outline_color: Literal[BACKGROUND_COLORS] = None
    background_color: Literal[BACKGROUND_COLORS] = None
    is_optional: bool = None


class ImageOptions(BaseModel):
    width: str = None
    height: str = None
    image_url: str = None
    is_optional: bool = None


class MoreOptionsLayout(BaseModel):
    name: str = None
    type: str = None
    options: dict = None


class MoreOptions(BaseModel):
    sub_key: str = None
    layout: MoreOptionsLayout = None


class TextField(BaseModel):
    name: str
    key: str
    type: str = "text"
    enums: Union[dict, None] = None
    data_type: str = None
    reference: Union[dict, None] = None
    options: Union[dict, None] = None


class DictField(BaseModel):
    name: str
    key: str
    type: str = "dict"
    options: Union[dict, None] = None


class SizeField(BaseModel):
    name: str
    key: str
    type: str = "size"
    options: SizeOptions = None


class ProgressField(BaseModel):
    name: str
    key: str
    type: str = "progress"
    options: ProgressOptions = None


class DatetimeField(BaseModel):
    name: str
    key: str
    type: str = "datetime"
    options: DatetimeOptions = None


class StateField(BaseModel):
    name: str
    key: str
    type: str = "state"
    options: StateOptions = None


class BadgeField(BaseModel):
    name: str
    key: str
    type: str = "badge"
    options: BadgeOptions = None


class ImageField(BaseModel):
    name: str
    key: str
    type: str = "image"
    options: ImageOptions = None


class EnumField(BaseModel):
    name: str
    key: str
    type: str = "enum"
    options: dict


class MoreField(BaseModel):
    name: str
    key: str
    type: str = "more"
    options: MoreOptions = None


class EnumBadgeField(BaseModel):
    type: str = "badge"
    options: BadgeOptions = None


class EnumStateField(BaseModel):
    type: str = "state"
    options: StateOptions = None


class EnumDatetimeField(BaseModel):
    type: str = "datetime"
    options: DatetimeOptions = None


class EnumImageField(BaseModel):
    type: str = "image"
    options: ImageOptions = None
