from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class CollectorRuleCondition(EmbeddedDocument):
    key = StringField(required=True)
    value = StringField(required=True)
    operator = StringField(choices=("eq", "contain", "not", "not_contain"))


class CollectorRuleOptions(EmbeddedDocument):
    stop_processing = BooleanField(default=False)


class CollectorRule(MongoModel):
    collector_rule_id = StringField(
        max_length=40, generate_id="collector-rule", unique=True
    )
    name = StringField(max_length=255, default="")
    rule_type = StringField(
        max_length=255, default="CUSTOM", choices=("MANAGED", "CUSTOM")
    )
    order = IntField(required=True)
    conditions = ListField(EmbeddedDocumentField(CollectorRuleCondition), default=[])
    conditions_policy = StringField(max_length=20, choices=("ALL", "ANY", "ALWAYS"))
    actions = DictField(default={})
    options = EmbeddedDocumentField(CollectorRuleOptions, default=CollectorRuleOptions)
    tags = DictField(default={})
    collector = ReferenceField("Collector", reverse_delete_rule=CASCADE)
    collector_id = StringField(max_length=40)
    resource_group = StringField(max_length=40, choices=("DOMAIN", "WORKSPACE"))
    workspace_id = StringField(max_length=40)
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)

    meta = {
        "updatable_fields": [
            "name",
            "order",
            "conditions",
            "conditions_policy",
            "actions",
            "options",
            "tags",
        ],
        "minimal_fields": [
            "collector_rule_id",
            "name",
            "order",
            "rule_type",
            "collector_id",
        ],
        "ordering": ["order"],
        "indexes": [
            "rule_type",
            "conditions_policy",
            "collector_id",
            "workspace_id",
            "domain_id",
        ],
    }
