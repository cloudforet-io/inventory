from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class RecordDiff(EmbeddedDocument):
    key = StringField(required=True)
    before = DynamicField(default=None, null=True)
    after = DynamicField(default=None, null=True)
    type = StringField(
        max_length=20, choices=("ADDED", "CHANGED", "DELETED"), required=True
    )

    def to_dict(self):
        return dict(self.to_mongo())


class Record(MongoModel):
    record_id = StringField(max_length=40, generate_id="record", unique=True)
    action = StringField(
        max_length=20, choices=("CREATE", "UPDATE", "DELETE"), required=True
    )
    diff = ListField(EmbeddedDocumentField(RecordDiff), default=[])
    diff_count = IntField(default=0)
    cloud_service_id = StringField(max_length=40, required=True)
    updated_by = StringField(max_length=40, choices=("COLLECTOR", "USER"))
    collector_id = StringField(max_length=40, default=None, null=True)
    job_id = StringField(max_length=40, default=None, null=True)
    user_id = StringField(max_length=255, default=None, null=True)
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now=True)

    meta = {
        "minimal_fields": [
            "record_id",
            "action",
            "diff_count",
            "cloud_service_id",
            "updated_by",
            "user_id",
            "collector_id",
            "job_id",
        ],
        "ordering": ["-created_at"],
        "indexes": [
            {
                "fields": ["domain_id", "cloud_service_id", "-created_at", "diff.key"],
                "name": "COMPOUND_INDEX_FOR_SEARCH",
            },
            {"fields": ["domain_id", "record_id"], "name": "COMPOUND_INDEX_FOR_GET"},
            "collector_id",
            "job_id",
            "domain_id",
        ],
    }
