from pydantic import BaseModel


class Sort(BaseModel):
    key: str
    desc: bool = False


class Options(BaseModel):
    default_sort: Sort
    fields: list


class BaseDynamicView(BaseModel):
    type: str
    options: dict = {}


class MainTableDynamicView(BaseDynamicView):
    name: str = 'Main Table'
    type: str = 'query-search-table'
    options: dict = {}
