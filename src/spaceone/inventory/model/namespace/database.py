from mongoengine import *
from spaceone.core.model.mongo_model import MongoModel


class Namespace(MongoModel):
    namespace_id = StringField(max_length=40, unique_with="domain_id")
    name = StringField(max_length=40)
    category = StringField(max_length=40)
    provider = StringField(max_length=40)
    icon = StringField(default=None, null=True)
    tags = DictField(default=None)
    is_managed = BooleanField(default=False)
    version = StringField(max_length=40, default=None, null=True)
    plugin_id = StringField(max_length=40, default=None, null=True)
    domain_id = StringField(max_length=40)
    workspaces = ListField(StringField(max_length=40))
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        "updatable_fields": [
            "name",
            "icon",
            "tags",
            "version",
            "workspaces",
            "updated_at",
        ],
        "minimal_fields": [
            "namespace_id",
            "name",
            "category",
            "provider",
        ],
        "ordering": ["name"],
        "change_query_keys": {
            "workspace_id": "workspaces",
        },
        "indexes": [
            "category",
            "provider",
            "is_managed",
            "domain_id",
            "workspaces",
        ],
    }
