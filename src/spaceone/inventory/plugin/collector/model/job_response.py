from typing import List
from pydantic import BaseModel

__all__ = ['TasksResponse']


class Task(BaseModel):
    task_options: dict


class TasksResponse(BaseModel):
    tasks: List[Task]
