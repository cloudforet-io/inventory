from spaceone.core.manager import BaseManager
from spaceone.inventory.connector.identity_connector import IdentityConnector


class IdentityManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # self.identity_conn: IdentityConnector = self.locator.get_connector('IdentityConnector')
        self.identity_conn: SpaceConnector = self.locator.get_connector('SpaceConnector', service='identity')

    def get_user(self, user_id, domain_id):
        return self.identity_conn.dispatch('User.get', {'user_id': user_id, 'domain_id': domain_id})
        # return self.identity_conn.get_user(user_id, domain_id)

    def list_users(self, query, domain_id):
        return self.identity_conn.dispatch('User.list', {'query': query, 'domain_id': domain_id})
        # return self.identity_conn.list_users(query, domain_id)

    def get_project(self, project_id, domain_id):
        return self.identity_conn.dispatch('Project.get', {'project_id': project_id, 'domain_id': domain_id})
        # return self.identity_conn.get_project(project_id, domain_id)

    def list_projects(self, query, domain_id):
        # return self.identity_conn.list_projects(query, domain_id)
        return self.identity_conn.dispatch('Project.list', {'query': query, 'domain_id': domain_id})
