from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel
from spaceone.inventory.model.cloud_service_type_model import CloudServiceType
from spaceone.inventory.model.region_model import Region


class CloudServiceStats(MongoModel):
    query_set_id = StringField(max_length=40, required=True)
    status = StringField(
        max_length=20, default="IN_PROGRESS", choices=("IN_PROGRESS", "DONE")
    )
    data = DictField()
    unit = DictField()
    provider = StringField(max_length=255)
    cloud_service_group = StringField(max_length=255)
    cloud_service_type = StringField(max_length=255)
    ref_cloud_service_type = StringField(max_length=255)
    region_code = StringField(max_length=255, default=None, null=True)
    ref_region = StringField(max_length=255, default=None, null=True)
    account = StringField(max_length=255, default=None, null=True)
    additional_info = DictField()
    project_id = StringField(max_length=40)
    workspace_id = StringField(max_length=40)
    domain_id = StringField(max_length=40)
    created_year = StringField(max_length=20, required=True)
    created_month = StringField(max_length=20, required=True)
    created_date = StringField(max_length=20, required=True)

    meta = {
        "updatable_fields": [],
        "minimal_fields": [
            "query_set_id",
            "data",
            "unit",
            "provider",
            "cloud_service_group",
            "cloud_service_type",
            "project_id",
            "created_date",
        ],
        "change_query_keys": {"user_projects": "project_id"},
        "reference_query_keys": {
            "ref_cloud_service_type": {
                "model": CloudServiceType,
                "foreign_key": "ref_cloud_service_type",
            },
            "ref_region": {"model": Region, "foreign_key": "ref_region"},
        },
        "indexes": [
            {
                "fields": [
                    "domain_id",
                    "workspace_id",
                    "query_set_id",
                    "status",
                    "-created_date",
                    "project_id",
                ],
                "name": "COMPOUND_INDEX_FOR_SEARCH",
            },
            {
                "fields": [
                    "domain_id",
                    "workspace_id",
                    "query_set_id",
                    "status",
                    "-created_month",
                ],
                "name": "COMPOUND_INDEX_FOR_SYNC_JOB",
            },
            {
                "fields": ["domain_id", "query_set_id"],
                "name": "COMPOUND_INDEX_FOR_DELETE",
            },
        ],
    }


class MonthlyCloudServiceStats(MongoModel):
    query_set_id = StringField(max_length=40, required=True)
    status = StringField(
        max_length=20, default="IN_PROGRESS", choices=("IN_PROGRESS", "DONE")
    )
    data = DictField()
    unit = DictField()
    provider = StringField(max_length=255)
    cloud_service_group = StringField(max_length=255)
    cloud_service_type = StringField(max_length=255)
    ref_cloud_service_type = StringField(max_length=255)
    region_code = StringField(max_length=255, default=None, null=True)
    ref_region = StringField(max_length=255, default=None, null=True)
    account = StringField(max_length=255, default=None, null=True)
    additional_info = DictField()
    project_id = StringField(max_length=40)
    workspace_id = StringField(max_length=40)
    domain_id = StringField(max_length=40)
    created_year = StringField(max_length=20)
    created_month = StringField(max_length=20)

    meta = {
        "updatable_fields": [],
        "change_query_keys": {"user_projects": "project_id"},
        "reference_query_keys": {
            "ref_cloud_service_type": {
                "model": CloudServiceType,
                "foreign_key": "ref_cloud_service_type",
            },
            "ref_region": {"model": Region, "foreign_key": "ref_region"},
        },
        "indexes": [
            {
                "fields": [
                    "domain_id",
                    "workspace_id",
                    "query_set_id",
                    "status",
                    "-created_month",
                    "project_id",
                ],
                "name": "COMPOUND_INDEX_FOR_SEARCH",
            },
            {
                "fields": [
                    "domain_id",
                    "workspace_id",
                    "query_set_id",
                    "status",
                    "-created_year",
                ],
                "name": "COMPOUND_INDEX_FOR_SYNC_JOB",
            },
            {
                "fields": ["domain_id", "query_set_id"],
                "name": "COMPOUND_INDEX_FOR_DELETE",
            },
        ],
    }


class CloudServiceStatsQueryHistory(MongoModel):
    query_hash = StringField(max_length=255)
    query_options = DictField(default={})
    query_set_id = StringField(max_length=40)
    domain_id = StringField(max_length=40)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        "updatable_fields": ["updated_at"],
        "indexes": [
            {
                "fields": ["domain_id", "query_set_id", "query_hash"],
                "name": "COMPOUND_INDEX_FOR_SEARCH",
            },
        ],
    }
