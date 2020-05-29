import logging

from spaceone.core.manager import BaseManager

_LOGGER = logging.getLogger(__name__)


"""
Schedule
"""
class ScheduleManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.schedule_model = self.locator.get_model('Schedule')

    def create_schedule(self, params):
        def _rollback(schedule_vo):
            _LOGGER.info(f'[ROLLBACK] Delete schedule : {schedule_vo.name} ({schedule_vo.schedule_id})')
            schedule_vo.delete()

        schedule_vo = self.schedule_model.create(params)
        self.transaction.add_rollback(_rollback, schedule_vo)
        return schedule_vo

    def delete_schedule(self, schedule_id, domain_id):
        schedule_vo = self.schedule_model.get(schedule_id=schedule_id, domain_id=domain_id)
        schedule_vo.delete()

    def get_schedule(self, schedule_id, domain_id):
        schedule_vo = self.schedule_model.get(schedule_id=schedule_id, domain_id=domain_id)
        return schedule_vo

    def list_schedules(self, query):
        return self.schedule_model.query(**query)

    def delete_by_collector_id(self, collector_id, domain_id):
        """ Delete all schedules related with collector
        """
        query = {
            'filter': [
                    {'k': 'collector_id', 'v': collector_id, 'o': 'eq'},
                    {'k': 'domain_id', 'v': domain_id, 'o': 'eq'}
            ]
        }
        schedule_vos, total_count = self.list_schedules(query)
        _LOGGER.debug(f'[delete_by_collector_id] found: {total_count}')
        for schedule_vo in schedule_vos:
            _LOGGER.debug(f'[delete_by_collector_id] delete schedule: {schedule_vo.schedule_id}')
            self.delete_schedule(schedule_vo.schedule_id, domain_id)
