# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Expire unclaimed GTF tasks whose runnable queue wait timed out."""

import logging
from typing import Any, cast
from uuid import UUID

from flask import current_app
from superset_core.tasks.types import TaskProperties, TaskStatus

from superset import db
from superset.commands.base import BaseCommand
from superset.daos.tasks import TaskDAO
from superset.extensions import celery_app
from superset.models.tasks import Task
from superset.stats_logger import BaseStatsLogger
from superset.tasks.utils import parse_properties

logger = logging.getLogger(__name__)

PENDING_ERROR_MESSAGE = (
    "Task expired: pending queue wait timed out before worker pickup"
)


class ExpirePendingTasksCommand(BaseCommand):
    """Fail expired unclaimed tasks independently of worker-heartbeat recovery.

    Active prerequisites suspend expiry, and the final prerequisite's completion
    grants a full queue budget. The DAO rechecks eligibility atomically so a
    worker claim wins over a stale scan. Only a committed FAILURE triggers
    notifications and non-terminating delivery revocation; no warehouse query
    cancellation is attempted for work that has never been claimed.
    """

    def run(self) -> int:
        timeout = current_app.config.get("GTF_PENDING_TASK_TIMEOUT")
        if timeout is None:
            return 0

        stats_logger: BaseStatsLogger = current_app.config["STATS_LOGGER"]
        uuids = TaskDAO.find_expired_pending(timeout)
        expired = 0
        for task_uuid in uuids:
            try:
                if self._expire(task_uuid, timeout, stats_logger):
                    expired += 1
            except Exception:  # noqa: BLE001 pylint: disable=broad-except
                db.session.rollback()  # pylint: disable=consider-using-transaction
                stats_logger.incr("gtf.task.pending_expire_error")
                logger.exception("Failed to expire pending task %s", task_uuid)
        if uuids:
            logger.info(
                "Pending expiry processed %d task(s), expired %d", len(uuids), expired
            )
        return expired

    def _expire(
        self, task_uuid: UUID, timeout: int, stats_logger: BaseStatsLogger
    ) -> bool:
        """Expire one candidate only if its full pending eligibility still holds."""
        properties = cast(
            TaskProperties,
            dict(
                parse_properties(
                    db.session.query(Task.properties)
                    .filter(Task.uuid == task_uuid)
                    .scalar()
                )
            ),
        )
        properties["error_message"] = PENDING_ERROR_MESSAGE
        # Preserve subscription state and both framework/task private handles.
        private = cast("dict[str, Any]", dict(properties.get("private") or {}))
        framework = {
            **(private.get("framework") or {}),
            "exception_type": "PendingTaskTimeoutError",
        }
        private["framework"] = framework
        properties["private"] = cast("Any", private)

        if not TaskDAO.fail_expired_pending(task_uuid, timeout, properties):
            db.session.rollback()  # pylint: disable=consider-using-transaction
            return False

        db.session.commit()  # pylint: disable=consider-using-transaction
        self._revoke(framework.get("celery_task_id"), stats_logger)

        from superset.tasks.manager import TaskManager

        TaskManager.publish_completion(task_uuid, TaskStatus.FAILURE.value)
        TaskManager.publish_entity_change(task_uuid)
        TaskManager.publish_required_by_changed(task_uuid)
        stats_logger.incr("gtf.task.pending_expired")
        logger.warning("Expired unclaimed pending task %s", task_uuid)
        return True

    def _revoke(
        self, celery_task_id: str | None, stats_logger: BaseStatsLogger
    ) -> None:
        """Best-effort revoke queued deliveries without terminating a worker."""
        if not celery_task_id:
            return
        try:
            celery_app.control.revoke(celery_task_id)
            stats_logger.incr("gtf.task.revoke")
        except Exception:  # noqa: BLE001 pylint: disable=broad-except
            stats_logger.incr("gtf.task.revoke_failure")
            logger.warning(
                "Failed to revoke Celery task %s", celery_task_id, exc_info=True
            )

    def validate(self) -> None:
        """No additional validation is required for this maintenance command."""
