from mongoengine import *
from spaceone.core.model.mongo_model import MongoModel


class Error(EmbeddedDocument):
    error_code = StringField()
    message = StringField()
    additional = DictField()


class JobTask(MongoModel):
    job_task_id = StringField(max_length=40, generate_id="job-task", unique=True)
    status = StringField(
        max_length=20,
        default="PENDING",
        choices=("PENDING", "CANCELED", "IN_PROGRESS", "SUCCESS", "FAILURE"),
    )
    provider = StringField(max_length=40, default=None, null=True)
    total_sub_tasks = IntField(default=0)
    remained_sub_tasks = IntField(default=0)
    created_count = IntField(default=0)
    updated_count = IntField(default=0)
    deleted_count = IntField(default=0)
    disconnected_count = IntField(default=0)
    failure_count = IntField(default=0)
    total_count = IntField(default=0)
    errors = ListField(EmbeddedDocumentField(Error, default=None, null=True))
    job_id = StringField(max_length=40)
    secret_id = StringField(max_length=40)
    collector_id = StringField(max_length=40)
    service_account_id = StringField(max_length=40)
    project_id = StringField(max_length=40)
    workspace_id = StringField(max_length=40)
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)
    started_at = DateTimeField(default=None, null=True)
    finished_at = DateTimeField(default=None, null=True)

    meta = {
        "updatable_fields": [
            "status",
            "provider",
            "remained_sub_tasks",
            "created_count",
            "updated_count",
            "deleted_count",
            "disconnected_count",
            "failure_count",
            "errors",
            "started_at",
            "finished_at",
        ],
        "minimal_fields": [
            "job_task_id",
            "status",
            "created_count",
            "updated_count",
            "deleted_count",
            "disconnected_count",
            "failure_count",
            "job_id",
            "created_at",
            "started_at",
            "finished_at",
        ],
        "change_query_keys": {"user_projects": "project_id"},
        "ordering": ["-created_at"],
        "indexes": [
            {
                "fields": ["domain_id", "collector_id", "status"],
                "name": "COMPOUND_INDEX_FOR_GC_1",
            },
            {
                "fields": ["domain_id", "job_id"],
                "name": "COMPOUND_INDEX_FOR_GC_2",
            },
            {
                "fields": ["domain_id", "-created_at", "status"],
                "name": "COMPOUND_INDEX_FOR_GC_3",
            },
            {
                "fields": ["domain_id", "workspace_id", "project_id"],
                "name": "COMPOUND_INDEX_FOR_SEARCH_1",
            },
            "status",
            "job_id",
            "collector_id",
            "project_id",
            "workspace_id",
            "domain_id",
        ],
    }
