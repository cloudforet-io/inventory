from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class CloudServiceType(MongoModel):
    cloud_service_type_id = StringField(
        max_length=40, generate_id="cloud-svc-type", unique=True
    )
    name = StringField(
        max_length=255, unique_with=["provider", "group", "workspace_id", "domain_id"]
    )
    provider = StringField(max_length=255)
    group = StringField(max_length=255)
    cloud_service_type_key = StringField(max_length=255)
    ref_cloud_service_type = StringField(max_length=255)
    service_code = StringField(max_length=255, default=None, null=True)
    is_primary = BooleanField(default=False)
    is_major = BooleanField(default=False)
    resource_type = StringField(max_length=255)
    labels = ListField(StringField(max_length=255))
    metadata = DictField()
    tags = DictField()
    workspace_id = StringField(max_length=40)
    domain_id = StringField(max_length=40)
    updated_by = StringField(default=None, null=True)
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        "updatable_fields": [
            "cloud_service_type_key",
            "service_code",
            "is_primary",
            "is_major",
            "resource_type",
            "metadata",
            "labels",
            "tags",
            "updated_by",
            "updated_at",
        ],
        "minimal_fields": [
            "cloud_service_type_id",
            "name",
            "provider",
            "group",
            "service_code",
            "is_primary",
            "is_major",
            "resource_type",
        ],
        "ordering": ["provider", "group", "name"],
        "indexes": [
            {
                "fields": ["domain_id", "-updated_at", "updated_by"],
                "name": "COMPOUND_INDEX_FOR_GC_1",
            },
            {
                "fields": ["domain_id", "workspace_id", "cloud_service_type_id"],
                "name": "COMPOUND_INDEX_FOR_SEARCH_1",
            },
            {
                "fields": [
                    "domain_id",
                    "workspace_id",
                    "provider",
                    "group",
                    "name",
                    "is_primary",
                ],
                "name": "COMPOUND_INDEX_FOR_SEARCH_2",
            },
            {
                "fields": ["domain_id", "workspace_id", "cloud_service_type_key"],
                "name": "COMPOUND_INDEX_FOR_SEARCH_3",
            },
            {
                "fields": ["cloud_service_type_id", "ref_cloud_service_type"],
                "name": "COMPOUND_INDEX_FOR_REF_1",
            },
            {
                "fields": ["labels", "is_primary", "ref_cloud_service_type"],
                "name": "COMPOUND_INDEX_FOR_REF_2",
            },
            "ref_cloud_service_type",
            "workspace_id",
            "domain_id",
        ],
    }
