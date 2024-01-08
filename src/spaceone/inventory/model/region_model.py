from mongoengine import *
from spaceone.core.model.mongo_model import MongoModel


class Region(MongoModel):
    region_id = StringField(max_length=40, generate_id="region", unique=True)
    name = StringField(max_length=255)
    region_key = StringField(max_length=255)
    region_code = StringField(
        max_length=255, unique_with=["provider", "workspace_id", "domain_id"]
    )
    provider = StringField(max_length=255)
    ref_region = StringField(max_length=255)
    tags = DictField()
    workspace_id = StringField(max_length=40)
    domain_id = StringField(max_length=40)
    updated_by = StringField(default=None, null=True)
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        "updatable_fields": ["name", "tags", "updated_by", "updated_at"],
        "minimal_fields": ["region_id", "name", "region_code", "provider"],
        "ordering": ["name"],
        "indexes": [
            {
                "fields": ["domain_id", "-updated_at", "updated_by"],
                "name": "COMPOUND_INDEX_FOR_GC_1",
            },
            {
                "fields": ["domain_id", "workspace_id", "region_id"],
                "name": "COMPOUND_INDEX_FOR_SEARCH_1",
            },
            {
                "fields": ["domain_id", "workspace_id", "provider", "region_code"],
                "name": "COMPOUND_INDEX_FOR_SEARCH_2",
            },
            {
                "fields": ["domain_id", "workspace_id", "region_key"],
                "name": "COMPOUND_INDEX_FOR_SEARCH_3",
            },
            {"fields": ["region_id", "ref_region"], "name": "COMPOUND_INDEX_FOR_REF_1"},
            {
                "fields": ["region_code", "provider", "ref_region"],
                "name": "COMPOUND_INDEX_FOR_REF_2",
            },
            "ref_region",
            "workspace_id",
            "domain_id",
        ],
    }
