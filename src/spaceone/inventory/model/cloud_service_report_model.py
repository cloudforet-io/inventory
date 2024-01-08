from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class ReportSchedule(EmbeddedDocument):
    state = StringField(
        max_length=20, default="ENABLED", choices=("ENABLED", "DISABLED")
    )
    hours = ListField(IntField(min_value=0, max_value=23), default=[])
    days_of_week = ListField(
        StringField(choices=("MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN")),
        default=[],
    )


class CloudServiceReport(MongoModel):
    report_id = StringField(max_length=40, generate_id="report", unique=True)
    name = StringField(max_length=255, required=True, unique_with="domain_id")
    options = ListField(DictField())
    file_format = StringField(max_length=20, default="EXCEL", choices=("EXCEL", "CSV"))
    schedule = EmbeddedDocumentField(ReportSchedule, required=True)
    target = DictField()
    language = StringField(max_length=10, default="en")
    timezone = StringField(max_length=255, default="UTC")
    tags = DictField()
    resource_group = StringField(max_length=40, choices=("DOMAIN", "WORKSPACE"))
    workspace_id = StringField(max_length=40)
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)
    last_sent_at = DateTimeField(default=None, null=True)

    meta = {
        "updatable_fields": [
            "name",
            "options",
            "file_format",
            "schedule",
            "target",
            "language",
            "timezone",
            "tags",
            "last_sent_at",
        ],
        "minimal_fields": [
            "report_id",
            "name",
            "file_format",
            "schedule",
            "last_sent_at",
        ],
        "ordering": ["name"],
        "indexes": ["resource_group", "workspace_id", "domain_id"],
    }
