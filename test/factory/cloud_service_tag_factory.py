import factory
from spaceone.core import utils

from spaceone.inventory.model import CloudServiceTag


class CloudServiceTagFactory(factory.mongoengine.MongoEngineFactory):
    class Meta:
        model = CloudServiceTag

    cloud_service_id = factory.LazyAttribute(lambda o: utils.generate_id('cloud-svc'))
    k = utils.generate_id()
    v = utils.generate_id()
    provider = 'aws'
    project_id = ''
    domain_id = utils.generate_id('domain')
    created_at = factory.Faker('date_time')