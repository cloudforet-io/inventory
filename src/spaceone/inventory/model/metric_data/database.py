from mongoengine import *
from spaceone.core.model.mongo_model import MongoModel


class MetricData(MongoModel):
    metric_id = StringField(max_length=80)
    metric_job_id = StringField(max_length=40)
    status = StringField(
        max_length=20, default="IN_PROGRESS", choices=["IN_PROGRESS", "DONE"]
    )
    value = FloatField(default=0)
    unit = StringField(default=None)
    labels = DictField(default=None)
    namespace_id = StringField(max_length=80)
    service_account_id = StringField(max_length=40, default=None, null=True)
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
            "service_account_id",
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
                    "metric_id",
                    "status",
                    "-created_date",
                    "workspace_id",
                    "project_id",
                    "service_account_id",
                ],
                "name": "COMPOUND_INDEX_FOR_SEARCH_1",
            },
            {
                "fields": [
                    "domain_id",
                    "metric_id",
                    "created_month",
                    "metric_job_id",
                ],
                "name": "COMPOUND_INDEX_FOR_SYNC_JOB_1",
            },
            {
                "fields": [
                    "domain_id",
                    "metric_id",
                    "metric_job_id",
                    "status",
                    "-created_date",
                ],
                "name": "COMPOUND_INDEX_FOR_SYNC_JOB_2",
            },
        ],
    }


class MonthlyMetricData(MongoModel):
    metric_id = StringField(max_length=80)
    metric_job_id = StringField(max_length=40)
    status = StringField(
        max_length=20, default="IN_PROGRESS", choices=["IN_PROGRESS", "DONE"]
    )
    value = FloatField(default=0)
    unit = StringField(default=None)
    labels = DictField(default=None)
    namespace_id = StringField(max_length=40)
    service_account_id = StringField(max_length=40)
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
                    "metric_id",
                    "status",
                    "-created_month",
                    "workspace_id",
                    "project_id",
                    "service_account_id",
                ],
                "name": "COMPOUND_INDEX_FOR_SEARCH_1",
            },
            {
                "fields": [
                    "domain_id",
                    "metric_id",
                    "created_year",
                ],
                "name": "COMPOUND_INDEX_FOR_SYNC_JOB_1",
            },
            {
                "fields": [
                    "domain_id",
                    "metric_id",
                    "metric_job_id",
                    "status",
                    "-created_month",
                ],
                "name": "COMPOUND_INDEX_FOR_SYNC_JOB_2",
            },
        ],
    }


class MetricQueryHistory(MongoModel):
    metric_id = StringField(max_length=80)
    domain_id = StringField(max_length=40)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        "updatable_fields": ["updated_at"],
        "indexes": [
            {
                "fields": ["domain_id", "metric_id"],
                "name": "COMPOUND_INDEX_FOR_SEARCH",
            },
        ],
    }
