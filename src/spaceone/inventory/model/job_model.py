from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel
from spaceone.inventory.model.collector_model import Collector


class Job(MongoModel):
    job_id = StringField(max_length=40, generate_id="job", unique=True)
    status = StringField(
        max_length=20,
        default="IN_PROGRESS",
        choices=("CANCELED", "IN_PROGRESS", "FAILURE", "SUCCESS"),
    )
    total_tasks = IntField(min_value=0, default=0)
    remained_tasks = IntField(default=0)
    success_tasks = IntField(min_value=0, default=0)
    failure_tasks = IntField(min_value=0, default=0)
    collector_id = StringField(max_length=40)
    secret_id = StringField(max_length=40, null=True, default=None)
    plugin_id = StringField(max_length=40)
    resource_group = StringField(max_length=40, choices=("DOMAIN", "WORKSPACE"))
    workspace_id = StringField(max_length=40)
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)
    finished_at = DateTimeField(default=None, null=True)
    mark_error = IntField(min_value=0, default=0)

    meta = {
        "updatable_fields": [
            "status",
            "total_tasks",
            "remained_tasks",
            "success_tasks",
            "failure_tasks",
            "collector_id",
            "updated_at",
            "finished_at",
            "mark_error",
        ],
        "minimal_fields": [
            "job_id",
            "status",
            "created_at",
            "finished_at",
        ],
        "reference_query_keys": {"collector": Collector},
        "ordering": ["-created_at"],
        "indexes": [
            "status",
            "collector_id",
            "plugin_id",
            "domain_id",
            "created_at",
            "finished_at",
        ],
    }
