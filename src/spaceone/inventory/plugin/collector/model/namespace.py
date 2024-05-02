from typing import Union, Literal
from pydantic import BaseModel

Category = Literal["COMMON", "ASSET", "SECURITY", "INFORMATION", "CUSTOM"]


class Namespace(BaseModel):
    namespace_id: Union[str, None] = None
    name: str
    category: Category
    provider: Union[str, None] = None
    icon: Union[str, None] = None
    tags: Union[dict, None] = {}
    version: str
