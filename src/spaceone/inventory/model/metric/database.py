from mongoengine import *
from spaceone.core.model.mongo_model import MongoModel


class Metric(MongoModel):
    metric_id = StringField(max_length=80, unique_with="domain_id")
    metric_job_id = StringField(max_length=40)
    name = StringField(max_length=80)
    status = StringField(max_length=20, choices=["IN_PROGRESS", "DONE"], default="DONE")
    metric_type = StringField(max_length=40, choices=["COUNTER", "GAUGE"])
    resource_type = StringField()
    query_options = DictField(required=True, default=None)
    date_field = StringField(default=None)
    unit = StringField(default=None)
    tags = DictField(default=None)
    labels_info = ListField(DictField())
    is_managed = BooleanField(default=False)
    is_new = BooleanField(default=True)
    version = StringField(max_length=40, default=None, null=True)
    plugin_id = StringField(max_length=40, default=None, null=True)
    namespace_id = StringField(max_length=80)
    resource_group = StringField(max_length=40, choices=("DOMAIN", "WORKSPACE"))
    domain_id = StringField(max_length=40)
    workspace_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        "updatable_fields": [
            "metric_job_id",
            "name",
            "status",
            "query_options",
            "date_field",
            "unit",
            "tags",
            "labels_info",
            "is_new",
            "version",
            "updated_at",
        ],
        "minimal_fields": [
            "metric_id",
            "name",
            "metric_type",
            "resource_type",
            "namespace_id",
        ],
        "ordering": ["namespace_id", "name"],
        "indexes": [
            {
                "fields": [
                    "domain_id",
                    "workspace_id",
                    "namespace_id",
                ],
                "name": "COMPOUND_INDEX_FOR_SEARCH_1",
            },
            "metric_type",
            "resource_type",
            "is_managed",
            "namespace_id",
        ],
    }
