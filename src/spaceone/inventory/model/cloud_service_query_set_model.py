from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel
from spaceone.inventory.model.cloud_service_type_model import CloudServiceType


class CloudServiceQuerySet(MongoModel):
    query_set_id = StringField(max_length=40, generate_id="query-set", unique=True)
    name = StringField(
        max_length=255,
        unique_with=[
            "provider",
            "cloud_service_group",
            "cloud_service_type",
            "workspace_id",
            "domain_id",
        ],
    )
    state = StringField(
        max_length=20, default="ENABLED", choices=("ENABLED", "DISABLED")
    )
    query_options = DictField()
    query_hash = StringField(max_length=255)
    query_type = StringField(
        max_length=20, required=True, choices=("MANAGED", "CUSTOM")
    )
    unit = DictField()
    additional_info_keys = ListField(StringField(max_length=255))
    data_keys = ListField(StringField(max_length=255))
    provider = StringField(max_length=255, default=None, null=True)
    cloud_service_group = StringField(max_length=255, default=None, null=True)
    cloud_service_type = StringField(max_length=255, default=None, null=True)
    ref_cloud_service_type = StringField(max_length=255, default=None, null=True)
    tags = DictField()
    resource_group = StringField(max_length=40, choices=("DOMAIN", "WORKSPACE"))
    workspace_id = StringField(max_length=40)
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        "updatable_fields": [
            "name",
            "state",
            "query_options",
            "query_hash",
            "unit",
            "additional_info_keys",
            "data_keys",
            "tags",
            "updated_at",
        ],
        "minimal_fields": [
            "query_set_id",
            "name",
            "state",
            "query_type",
            "provider",
            "cloud_service_group",
            "cloud_service_type",
        ],
        "ordering": ["provider", "cloud_service_group", "cloud_service_type"],
        "reference_query_keys": {
            "ref_cloud_service_type": {
                "model": CloudServiceType,
                "foreign_key": "ref_cloud_service_type",
            }
        },
        "indexes": [
            {
                "fields": [
                    "domain_id",
                    "workspace_id",
                    "provider",
                    "cloud_service_group",
                    "cloud_service_type",
                ],
                "name": "COMPOUND_INDEX_FOR_SEARCH_1",
            },
            "state",
            "query_hash",
            "query_type",
            "provider",
            "cloud_service_group",
            "cloud_service_type",
            "resource_group",
            "workspace_id",
            "domain_id",
        ],
    }
