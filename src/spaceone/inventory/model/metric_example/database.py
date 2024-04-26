from mongoengine import *
from spaceone.core.model.mongo_model import MongoModel


class MetricExample(MongoModel):
    example_id = StringField(max_length=40, generate_id="me", unique=True)
    name = StringField(max_length=40)
    options = DictField(default=None)
    tags = DictField(default=None)
    metric_id = StringField(max_length=40)
    namespace_id = StringField(max_length=40)
    user_id = StringField(max_length=40)
    workspace_id = StringField(max_length=40, default=None, null=True)
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        "updatable_fields": [
            "name",
            "options",
            "tags",
        ],
        "minimal_fields": [
            "example_id",
            "name",
            "metric_id",
            "user_id",
            "workspace_id",
        ],
        "ordering": ["name"],
        "indexes": [
            "name",
            "metric_id",
            "user_id",
            "workspace_id",
            "domain_id",
        ],
    }
