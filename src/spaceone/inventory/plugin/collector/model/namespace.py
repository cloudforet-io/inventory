from typing import Union, Literal
from pydantic import BaseModel

Category = Literal["ASSET", "SECURITY", "INFORMATION", "RECOMMENDATION"]


class Namespace(BaseModel):
    namespace_id: Union[str, None] = None
    name: str
    category: Category
    resource_type: str
    group: Union[str, None] = None
    icon: Union[str, None] = None
    tags: Union[dict, None] = {}
    version: str
