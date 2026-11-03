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
import abc
import json  # noqa: TID251 - logging bootstraps before application serialization
import logging
from datetime import datetime, timezone
from logging.handlers import TimedRotatingFileHandler

import flask.app
import flask.config

logger = logging.getLogger(__name__)


class JsonLogFormatter(logging.Formatter):
    """Keep each formatted message and its full traceback in one physical line."""

    def __init__(self, formatter: logging.Formatter | None = None) -> None:
        super().__init__()
        self.formatter = formatter or logging.Formatter("%(message)s")

    def format(self, record: logging.LogRecord) -> str:
        message = self.formatter.format(record)
        metadata = {
            "logger": record.name,
            "process_id": record.process,
            "process_name": record.processName,
            "thread_name": record.threadName,
        }
        for key in ("task_id", "task_name"):
            if isinstance(value := record.__dict__.get(key), str):
                metadata[key] = value
        return json.dumps(
            {
                "time": datetime.fromtimestamp(record.created, timezone.utc)
                .isoformat(timespec="milliseconds")
                .replace("+00:00", "Z"),
                "levelLabel": record.levelname,
                "message": message,
                "metadata": metadata,
            },
            separators=(",", ":"),
        )


def configure_json_logging(logger: logging.Logger) -> None:
    """Preserve handler destinations, levels and filters; only change framing."""
    for handler in logger.handlers:
        if isinstance(handler, logging.StreamHandler) and not isinstance(
            handler.formatter, JsonLogFormatter
        ):
            handler.setFormatter(JsonLogFormatter(handler.formatter))


class LoggingConfigurator(abc.ABC):  # pylint: disable=too-few-public-methods
    @abc.abstractmethod
    def configure_logging(
        self, app_config: flask.config.Config, debug_mode: bool
    ) -> None:
        pass


class DefaultLoggingConfigurator(  # pylint: disable=too-few-public-methods
    LoggingConfigurator
):
    def configure_logging(
        self, app_config: flask.config.Config, debug_mode: bool
    ) -> None:
        if app_config["SILENCE_FAB"]:
            logging.getLogger("flask_appbuilder").setLevel(logging.ERROR)

        # basicConfig() will set up a default StreamHandler on stderr
        logging.basicConfig(format=app_config["LOG_FORMAT"])
        logging.getLogger().setLevel(app_config["LOG_LEVEL"])

        # Route Python warnings through the logging framework so they get
        # proper log-level formatting instead of raw stderr output. Without
        # this, the warnings module writes multi-line text to stderr where
        # the source-code context line has no level prefix, causing log
        # aggregators to misclassify it as an error.
        logging.captureWarnings(True)

        if app_config["ENABLE_TIME_ROTATE"]:
            logging.getLogger().setLevel(app_config["TIME_ROTATE_LOG_LEVEL"])
            handler = TimedRotatingFileHandler(
                app_config["FILENAME"],
                when=app_config["ROLLOVER"],
                interval=app_config["INTERVAL"],
                backupCount=app_config["BACKUP_COUNT"],
            )
            logging.getLogger().addHandler(handler)

        if app_config.get("LOG_FORMAT_JSON"):
            configure_json_logging(logging.getLogger())

        logger.debug("logging was configured successfully")
