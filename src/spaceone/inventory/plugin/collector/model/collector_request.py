from typing import Union
from pydantic import BaseModel

__all__ = ["CollectorInitRequest", "CollectorVerifyRequest", "CollectorCollectRequest"]


class CollectorInitRequest(BaseModel):
    options: dict
    domain_id: Union[str, None] = None


class CollectorVerifyRequest(BaseModel):
    options: dict
    secret_data: dict
    domain_id: Union[str, None] = None


class CollectorCollectRequest(BaseModel):
    options: dict
    secret_data: dict
    domain_id: Union[str, None] = None
    task_options: Union[dict, None] = None
