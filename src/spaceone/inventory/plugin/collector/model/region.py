from pydantic import BaseModel


class Region(BaseModel):
    name: str = ""
    region_code: str
    provider: str
    tags: dict = {}
