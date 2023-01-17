import unittest
from unittest.mock import patch
from mongoengine import connect, disconnect

from spaceone.core.unittest.result import print_data
from spaceone.core.unittest.runner import RichTestRunner
from spaceone.core import config
from spaceone.core import utils
from spaceone.core.model.mongo_model import MongoModel
from spaceone.core.transaction import Transaction

from spaceone.inventory.service.collector_rule_service import CollectorRuleService
from spaceone.inventory.model.collector_rule_model import CollectorRule
from spaceone.inventory.manager.identity_manager import IdentityManager
from spaceone.inventory.info.collector_rule_info import *
from test.factory.collector_factory import CollectorFactory
from test.factory.collector_rule_factory import CollectorRuleFactory


class TestCollectorRuleService(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        config.init_conf(package='spaceone.inventory')
        connect('test', host='mongomock://localhost')

        cls.domain_id = utils.generate_id('domain')
        cls.metadata = {
            'verb': 'create',
            'collector_id': utils.generate_id('collector'),
            'job_id': utils.generate_id('job'),
            'plugin_id': utils.generate_id('plugin'),
            'secret.service_account_id': utils.generate_id('sa'),
        }

        cls.collector_vo = CollectorFactory(domain_id=cls.domain_id)

        super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        disconnect()

    @patch.object(MongoModel, 'connect', return_value=None)
    def tearDown(self, *args) -> None:
        print()
        print('(tearDown) ==> Delete collector rule')
        self.collector_vo.delete()

    @patch.object(IdentityManager, '__init__', return_value=None)
    @patch.object(MongoModel, 'connect', return_value=None)
    def test_create_collector_rule(self, *args):
        params = {
            'collector_id': self.collector_vo.collector_id,
            'name': 'Test Data Source Rule',
            'conditions_policy': 'ALWAYS',
            'actions': {
                'match_service_account': {
                    'source': 'account',
                    'target': 'data.account_id'
                }
            },
            'options': {},
            'tags': {
                utils.random_string(): utils.random_string()
            },
            'domain_id': self.domain_id
        }

        collector_rule_svc = CollectorRuleService(metadata=self.metadata)
        collector_rule_vo = collector_rule_svc.create(params.copy())
        print(CollectorRuleInfo(collector_rule_vo))
        print_data(collector_rule_vo.to_dict(), 'test_create_collector_rule')

    @patch.object(IdentityManager, '__init__', return_value=None)
    @patch.object(MongoModel, 'connect', return_value=None)
    def test_update_collector_rule(self, *args):
        collector_rule_vo = CollectorRuleFactory(domain_id=self.domain_id,
                                                 collector_id=self.collector_vo.collector_id,
                                                 collector=self.collector_vo)

        name = 'Update name'
        params = {
            'name': name,
            'collector_rule_id': collector_rule_vo.collector_rule_id,
            'tags': {
                utils.random_string(): utils.random_string()
            },
            'domain_id': self.domain_id
        }

        self.metadata.update({'verb': 'update'})
        collector_rule_svc = CollectorRuleService(metadata=self.metadata)
        update_collector_rule_vo = collector_rule_svc.update(params.copy())

        print_data(update_collector_rule_vo.to_dict(), 'test_update_collector_rule')
        print(CollectorRuleInfo(update_collector_rule_vo))

        self.assertEqual(name, update_collector_rule_vo.name)
        self.assertEqual(params['tags'], update_collector_rule_vo.tags)
        self.assertEqual(params['domain_id'], update_collector_rule_vo.domain_id)

    @patch.object(IdentityManager, '__init__', return_value=None)
    @patch.object(MongoModel, 'connect', return_value=None)
    def test_delete_collector_rule(self, *args):
        collector_rule_vo = CollectorRuleFactory(domain_id=self.domain_id)
        params = {
            'collector_rule_id': collector_rule_vo.collector_rule_id,
            'domain_id': self.domain_id
        }

        self.metadata.update({'verb': 'delete'})
        collector_rule_svc = CollectorRuleService(metadata=self.metadata)
        result = collector_rule_svc.delete(params)

        self.assertIsNone(result)

    @patch.object(IdentityManager, '__init__', return_value=None)
    @patch.object(MongoModel, 'connect', return_value=None)
    def test_get_collector_rule(self, *args):
        collector_rule_vo = CollectorRuleFactory(domain_id=self.domain_id)
        params = {
            'collector_rule_id': collector_rule_vo.collector_rule_id,
            'domain_id': self.domain_id
        }

        self.metadata.update({'verb': 'get'})
        collector_rule_svc = CollectorRuleService(metadata=self.metadata)
        get_collector_rule_vo = collector_rule_svc.get(params)

        print_data(get_collector_rule_vo.to_dict(), 'test_get_collector_rule')
        CollectorRuleInfo(get_collector_rule_vo)

        self.assertIsInstance(get_collector_rule_vo, CollectorRule)


if __name__ == "__main__":
    unittest.main(testRunner=RichTestRunner)
