import factory

from spaceone.core import utils
from spaceone.inventory.model.collector_rule_model import CollectorRule


class CollectorRuleFactory(factory.mongoengine.MongoEngineFactory):

    class Meta:
        model = CollectorRule

    order = 1
    conditions = []
    actions = {}
    collector = None
    collector_id = utils.generate_id('collector')

    collector_rule_id = factory.LazyAttribute(lambda o: utils.generate_id('collector-rule'))
    name = factory.LazyAttribute(lambda o: utils.random_string())
    conditions_policy = 'ALWAYS'
    rule_type = 'CUSTOM'
    options = {
        'stop_processing': True
    }
    tags = {
        'xxx': 'yy'
    }
    domain_id = utils.generate_id('domain')
    created_at = factory.Faker('date_time')
