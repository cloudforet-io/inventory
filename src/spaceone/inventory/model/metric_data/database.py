from mongoengine import *
from spaceone.core.model.mongo_model import MongoModel


class MetricData(MongoModel):
    metric_id = StringField(max_length=40)
    status = StringField(
        max_length=20, default="IN_PROGRESS", choices=["IN_PROGRESS", "DONE"]
    )
    value = FloatField(default=0)
    unit = StringField(default=None)
    labels = DictField(default=None)
    namespace_id = StringField(max_length=40)
    project_id = StringField(max_length=40)
    workspace_id = StringField(max_length=40)
    domain_id = StringField(max_length=40)
    created_year = StringField(max_length=4, required=True)
    created_month = StringField(max_length=7, required=True)
    created_date = StringField(max_length=10, required=True)

    meta = {
        "updatable_fields": [],
        "minimal_fields": [
            "metric_id",
            "value",
            "unit",
            "project_id",
            "workspace_id",
            "created_date",
        ],
        "change_query_keys": {
            "user_projects": "project_id",
        },
        "indexes": [
            {
                "fields": [
                    "domain_id",
                    "workspace_id",
                    "metric_id",
                    "status",
                    "-created_date",
                    "project_id",
                ],
                "name": "COMPOUND_INDEX_FOR_SEARCH_1",
            },
            {
                "fields": [
                    "domain_id",
                    "workspace_id",
                    "metric_id",
                    "status",
                    "-created_month",
                ],
                "name": "COMPOUND_INDEX_FOR_SYNC_JOB_1",
            },
            {
                "fields": [
                    "domain_id",
                    "metric_id",
                ],
                "name": "COMPOUND_INDEX_FOR_DELETE",
            },
        ],
    }


class MonthlyMetricData(MongoModel):
    metric_id = StringField(max_length=40)
    status = StringField(
        max_length=20, default="IN_PROGRESS", choices=["IN_PROGRESS", "DONE"]
    )
    value = FloatField(default=0)
    unit = StringField(default=None)
    labels = DictField(default=None)
    namespace_id = StringField(max_length=40)
    project_id = StringField(max_length=40)
    workspace_id = StringField(max_length=40)
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)
    created_year = StringField(max_length=4, required=True)
    created_month = StringField(max_length=7, required=True)

    meta = {
        "updatable_fields": [],
        "change_query_keys": {
            "user_projects": "project_id",
        },
        "indexes": [
            {
                "fields": [
                    "domain_id",
                    "workspace_id",
                    "metric_id",
                    "status",
                    "-created_month",
                    "project_id",
                ],
                "name": "COMPOUND_INDEX_FOR_SEARCH_1",
            },
            {
                "fields": [
                    "domain_id",
                    "workspace_id",
                    "metric_id",
                    "status",
                    "-created_year",
                ],
                "name": "COMPOUND_INDEX_FOR_SYNC_JOB_1",
            },
            {
                "fields": [
                    "domain_id",
                    "metric_id",
                ],
                "name": "COMPOUND_INDEX_FOR_DELETE_1",
            },
        ],
    }
