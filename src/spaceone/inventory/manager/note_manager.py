import logging
from typing import Tuple

from spaceone.core.model.mongo_model import QuerySet
from spaceone.core.manager import BaseManager
from spaceone.inventory.model.note_model import Note

_LOGGER = logging.getLogger(__name__)


class NoteManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.note_model: Note = self.locator.get_model("Note")

    def create_note(self, params: dict) -> Note:
        def _rollback(vo: Note):
            _LOGGER.info(f"[create_note._rollback] " f"Delete note: {vo.note_id}")
            vo.delete()

        note_vo: Note = self.note_model.create(params)
        self.transaction.add_rollback(_rollback, note_vo)

        return note_vo

    def update_note_by_vo(self, params: dict, note_vo: Note) -> Note:
        def _rollback(old_data):
            _LOGGER.info(
                f"[update_note_by_vo._rollback] Revert Data: " f'{old_data["note_id"]}'
            )
            note_vo.update(old_data)

        self.transaction.add_rollback(_rollback, note_vo.to_dict())

        return note_vo.update(params)

    @staticmethod
    def delete_note_by_vo(note_vo: Note) -> None:
        note_vo.delete()

    def get_note(
        self,
        note_id: str,
        domain_id: str,
        workspace_id: str = None,
        user_projects: list = None,
    ) -> Note:
        conditions = {
            "note_id": note_id,
            "domain_id": domain_id,
        }

        if workspace_id:
            conditions["workspace_id"] = workspace_id

        if user_projects:
            conditions["project_id"] = user_projects

        return self.note_model.get(**conditions)

    def filter_notes(self, **conditions) -> QuerySet:
        return self.note_model.filter(**conditions)

    def list_notes(self, query: dict) -> Tuple[QuerySet, int]:
        return self.note_model.query(**query)

    def stat_notes(self, query: dict) -> dict:
        return self.note_model.stat(**query)
