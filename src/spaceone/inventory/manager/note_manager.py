import logging

from spaceone.core.manager import BaseManager
from spaceone.inventory.model.note_model import Note

_LOGGER = logging.getLogger(__name__)


class NoteManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.note_model: Note = self.locator.get_model('Note')

    def create_note(self, params):
        def _rollback(note_vo):
            _LOGGER.info(f'[create_note._rollback] '
                         f'Delete note : {note_vo.note_id}')
            note_vo.delete()

        note_vo: Note = self.note_model.create(params)
        self.transaction.add_rollback(_rollback, note_vo)

        return note_vo

    def update_note(self, params):
        note_vo: Note = self.get_note(params['note_id'], params['domain_id'])
        return self.update_note_by_vo(params, note_vo)

    def update_note_by_vo(self, params, note_vo):
        def _rollback(old_data):
            _LOGGER.info(f'[update_note_by_vo._rollback] Revert Data : '
                         f'{old_data["note_id"]}')
            note_vo.update(old_data)

        self.transaction.add_rollback(_rollback, note_vo.to_dict())

        return note_vo.update(params)

    def delete_note(self, note_id, domain_id):
        note_vo: Note = self.get_note(note_id, domain_id)
        note_vo.delete()

    def get_note(self, note_id, domain_id, only=None):
        return self.note_model.get(note_id=note_id, domain_id=domain_id, only=only)

    def filter_notes(self, **conditions):
        return self.note_model.filter(**conditions)

    def list_notes(self, query={}):
        return self.note_model.query(**query)

    def stat_notes(self, query):
        return self.note_model.stat(**query)
