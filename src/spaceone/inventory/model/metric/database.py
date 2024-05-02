from mongoengine import *
from spaceone.core.model.mongo_model import MongoModel


class Metric(MongoModel):
    metric_id = StringField(max_length=40, unique_with="domain_id")
    name = StringField(max_length=40)
    metric_type = StringField(max_length=40, choices=["COUNTER", "GAUGE"])
    resource_type = StringField()
    query_options = DictField(required=True, default=None)
    date_field = StringField(default=None)
    unit = StringField(default=None)
    tags = DictField(default=None)
    label_keys = ListField(DictField())
    is_managed = BooleanField(default=False)
    version = StringField(max_length=40, default=None, null=True)
    plugin_id = StringField(max_length=40, default=None, null=True)
    namespace_id = StringField(max_length=40)
    domain_id = StringField(max_length=40)
    workspaces = ListField(StringField(max_length=40))
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        "updatable_fields": [
            "name",
            "query_options",
            "date_field",
            "unit",
            "tags",
        ],
        "minimal_fields": [
            "metric_id",
            "name",
            "metric_type",
            "resource_type",
            "namespace_id",
        ],
        "ordering": ["namespace_id", "name"],
        "change_query_keys": {
            "workspace_id": "workspaces",
        },
        "indexes": [
            {
                "fields": [
                    "domain_id",
                    "workspaces",
                    "namespace_id",
                ],
                "name": "COMPOUND_INDEX_FOR_SEARCH_1",
            },
            "metric_type",
            "resource_type",
            "is_managed",
            "namespace_id",
            "domain_id",
            "workspaces",
        ],
    }
