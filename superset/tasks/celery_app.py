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

"""
This is the main entrypoint used by Celery workers. As such,
it needs to call create_app() in order to initialize things properly
"""

from logging import Logger
from typing import Any

from celery import Task
from celery.signals import (
    after_setup_logger,
    after_setup_task_logger,
    beat_init,
    task_postrun,
    worker_process_init,
    worker_process_shutdown,
)
from celery.utils.log import get_multiprocessing_logger
from flask import has_app_context

# Superset framework imports
from superset import create_app
from superset.extensions import celery_app, db
from superset.observability import init_process
from superset.observability.runtime import shutdown as shutdown_observability
from superset.utils.logging_configurator import configure_json_logging

# Init the Flask app / configure everything
flask_app = create_app()

# Need to import late, as the celery_app will have been setup by "create_app()"
# ruff: noqa: E402, F401
# pylint: disable=wrong-import-position, unused-import
from . import cache, scheduler, version_history_retention

# Export the celery app globally for Celery (as run on the cmd line) to find
app = celery_app


@after_setup_logger.connect
@after_setup_task_logger.connect
def configure_worker_json_logging(logger: Logger, **kwargs: Any) -> None:
    if flask_app.config.get("LOG_FORMAT_JSON"):
        configure_json_logging(logger)
        configure_json_logging(get_multiprocessing_logger())


@worker_process_init.connect
def reset_db_connection_pool(**kwargs: Any) -> None:  # pylint: disable=unused-argument
    init_process()
    with flask_app.app_context():
        # https://docs.sqlalchemy.org/en/14/core/connections.html#engine-disposal
        db.engine.dispose()


@beat_init.connect
def initialize_beat_observability(**kwargs: Any) -> None:
    """Beat is a standalone PID, not a prefork parent or Flask web process."""
    init_process(role="beat")


@worker_process_shutdown.connect
def shutdown_worker_observability(**kwargs: Any) -> None:
    """Flush only this child writer, with bounded exporter deadlines."""
    shutdown_observability()


@task_postrun.connect
def teardown(  # pylint: disable=unused-argument
    retval: Any,
    task: Task | None = None,
    *args: Any,
    **kwargs: Any,
) -> None:
    """
    After each Celery task teardown the Flask-SQLAlchemy session.

    Eager and direct invocations share their caller's session.

    :param retval: The return value of the task
    :see: https://docs.celeryq.dev/en/stable/userguide/signals.html#task-postrun
    :see: https://gist.github.com/twolfson/a1b329e9353f9b575131
    """

    if task is not None and (task.request.is_eager or task.request.called_directly):
        return
    if has_app_context():
        db.session.remove()
