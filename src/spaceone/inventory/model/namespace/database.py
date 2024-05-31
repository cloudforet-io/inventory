from mongoengine import *
from spaceone.core.model.mongo_model import MongoModel


class Namespace(MongoModel):
    namespace_id = StringField(max_length=80, unique_with="domain_id")
    name = StringField(max_length=40)
    category = StringField(max_length=40)
    resource_type = StringField(required=True)
    group = StringField(max_length=40, default="etc")
    icon = StringField(default=None, null=True)
    tags = DictField(default=None)
    is_managed = BooleanField(default=False)
    version = StringField(max_length=40, default=None, null=True)
    plugin_id = StringField(max_length=40, default=None, null=True)
    resource_group = StringField(max_length=40, choices=("DOMAIN", "WORKSPACE"))
    domain_id = StringField(max_length=40)
    workspace_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        "updatable_fields": [
            "name",
            "resource_type",
            "group",
            "icon",
            "tags",
            "version",
            "updated_at",
        ],
        "minimal_fields": [
            "namespace_id",
            "name",
            "category",
            resource_type,
            "group",
        ],
        "ordering": ["name"],
        "indexes": [
            "category",
            "resource_type",
            "group",
            "is_managed",
            "domain_id",
            "workspace_id",
        ],
    }
