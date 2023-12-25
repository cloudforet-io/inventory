import logging

from spaceone.core.service import *
from spaceone.inventory.model.record_model import Record
from spaceone.inventory.model.note_model import Note
from spaceone.inventory.model.cloud_service_model import CloudService
from spaceone.inventory.manager.record_manager import RecordManager
from spaceone.inventory.manager.note_manager import NoteManager
from spaceone.inventory.manager.cloud_service_manager import CloudServiceManager

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class NoteService(BaseService):
    resource = "Note"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.note_mgr: NoteManager = self.locator.get_manager("NoteManager")

    @transaction(
        permission="inventory:Note.write",
        role_types=["WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["record_id", "note", "domain_id"])
    def create(self, params: dict) -> Note:
        """Create note for record

        Args:
            params (dict): {
                'record_id': 'str',         # required
                'note': 'str',              # required
                'workspace_id': 'str',      # injected from auth (required)
                'domain_id': 'str'          # injected from auth (required)
                'user_projects': 'list'     # injected from auth
            }

        Returns:
            note_vo (object)
        """

        cloud_svc_mgr: CloudServiceManager = self.locator.get_manager(
            "CloudServiceManager"
        )
        record_mgr: RecordManager = self.locator.get_manager("RecordManager")
        record_vo: Record = record_mgr.get_record(
            params["record_id"], params["domain_id"]
        )

        cloud_svc_vo: CloudService = cloud_svc_mgr.get_cloud_service(
            record_vo.cloud_service_id,
            params["domain_id"],
            params["workspace_id"],
            params.get("user_projects"),
        )

        created_by = self.transaction.get_meta("authorization.user_id")

        params["cloud_service_id"] = cloud_svc_vo.cloud_service_id
        params["workspace_id"] = cloud_svc_vo.workspace_id
        params["project_id"] = cloud_svc_vo.project_id
        params["created_by"] = created_by

        return self.note_mgr.create_note(params)

    @transaction(
        permission="inventory:Note.write",
        role_types=["WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["note_id", "domain_id"])
    def update(self, params: dict) -> Note:
        """Update note for record

        Args:
            params (dict): {
                'note_id': 'str',           # required
                'note': 'dict',
                'workspace_id': 'str',      # injected from auth (required)
                'domain_id': 'str',         # injected from auth (required)
                'user_projects': 'list'     # injected from auth
            }

        Returns:
            note_vo (object)
        """

        note_vo = self.note_mgr.get_note(
            params["note_id"],
            params["domain_id"],
            params["workspace_id"],
            params.get("user_projects"),
        )

        return self.note_mgr.update_note_by_vo(params, note_vo)

    @transaction(
        permission="inventory:Note.write",
        role_types=["WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["note_id", "domain_id"])
    def delete(self, params: dict) -> None:
        """Delete note for record

        Args:
            params (dict): {
                'note_id': 'str',           # required
                'workspace_id': 'str',      # injected from auth (required)
                'domain_id': 'str',         # injected from auth (required)
                'user_projects': 'list'     # injected from auth
            }

        Returns:
            None
        """

        note_vo = self.note_mgr.get_note(
            params["note_id"],
            params["domain_id"],
            params["workspace_id"],
            params.get("user_projects"),
        )

        self.note_mgr.delete_note_by_vo(note_vo)

    @transaction(
        permission="inventory:Note.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["note_id", "domain_id"])
    def get(self, params: dict) -> Note:
        """Get record note

        Args:
            params (dict): {
                'note_id': 'str',           # required
                'workspace_id': 'str',      # injected from auth
                'domain_id': 'str',         # injected from auth (required)
                'user_projects': 'list'     # injected from auth
            }

        Returns:
            note_vo (object)
        """

        return self.note_mgr.get_note(
            params["note_id"], params["domain_id"], params.get("only")
        )

    @transaction(
        permission="inventory:Note.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["domain_id"])
    @append_query_filter(
        [
            "note_id",
            "created_by",
            "record_id",
            "cloud_service_id",
            "project_id",
            "workspace_id",
            "domain_id",
            "user_projects",
        ]
    )
    @append_keyword_filter(["note"])
    def list(self, params: dict):
        """List notes in record

        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.Query)',
                'note_id': 'str',
                'created_by': 'str',
                'record_id': 'str',
                'cloud_service_id': 'str',
                'project_id': 'str',
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str',             # injected from auth (required)
                'user_projects': 'list',        # injected from auth
            }

        Returns:
            note_vos (object)
            total_count (int)
        """

        query = params.get("query", {})
        return self.note_mgr.list_notes(query)

    @transaction(
        permission="inventory:Note.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["query", "domain_id"])
    @append_query_filter(["workspace_id", "domain_id", "user_projects"])
    @append_keyword_filter(["note"])
    def stat(self, params: dict) -> dict:
        """
        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)', # required
                'workspace_id': 'str',      # injected from auth
                'domain_id': 'str',         # injected from auth (required)
                'user_projects': 'list',    # injected from auth
            }

        Returns:
            values (list) : 'list of statistics data'

        """

        query = params.get("query", {})
        return self.note_mgr.stat_notes(query)
