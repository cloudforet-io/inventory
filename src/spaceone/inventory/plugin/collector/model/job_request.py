from typing import Union
from pydantic import BaseModel

__all__ = ['JobGetTaskRequest']


class JobGetTaskRequest(BaseModel):
    options: dict
    secret_data: dict
    domain_id: Union[str, None] = None