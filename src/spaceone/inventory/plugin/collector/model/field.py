from pydantic import BaseModel


class BaseField(BaseModel):
    name: str
    key: str


class TextField(BaseField):
    type: str = 'text'


class StateField(BaseField):
    pass
