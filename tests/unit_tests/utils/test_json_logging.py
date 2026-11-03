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
import io
import logging

import pytest
from flask.config import Config

from superset.utils import json
from superset.utils.logging_configurator import (
    configure_json_logging,
    DefaultLoggingConfigurator,
)


def logger_with_output() -> tuple[logging.Logger, io.StringIO]:
    stream = io.StringIO()
    logger = logging.Logger("json-framing", level=logging.INFO)
    handler = logging.StreamHandler(stream)
    handler.setFormatter(logging.Formatter("%(levelname)s:%(message)s"))
    logger.addHandler(handler)
    return logger, stream


def test_chained_traceback_is_one_record_and_preserves_the_next_message() -> None:
    logger, stream = logger_with_output()
    configure_json_logging(logger)
    try:
        try:
            raise ValueError("inner\nerror")
        except ValueError as error:
            raise RuntimeError("outer\nerror") from error
    except RuntimeError:
        logger.exception("operation %s failed\nwith context", "example")
    logger.info("following message")

    lines = stream.getvalue().splitlines()
    assert len(lines) == 2
    failure, following = [json.loads(line) for line in lines]
    assert failure["levelLabel"] == "ERROR"
    assert "operation example failed\nwith context" in failure["message"]
    assert "ValueError: inner\nerror" in failure["message"]
    assert "RuntimeError: outer\nerror" in failure["message"]
    assert "direct cause" in failure["message"]
    assert "test_chained_traceback" in failure["message"]
    assert failure["metadata"]["logger"] == "json-framing"
    assert following["message"] == "INFO:following message"


def test_repeated_setup_keeps_levels_filters_and_full_multiline_messages() -> None:
    logger, stream = logger_with_output()
    logger.handlers[0].addFilter(lambda record: record.getMessage() != "filtered")
    configure_json_logging(logger)
    configure_json_logging(logger)
    message = "unicode: café\n" + "x" * 120_000
    logger.debug("disabled debug")
    logger.info("filtered")
    logger.info(message)

    lines = stream.getvalue().splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["message"] == "INFO:" + message


def test_stack_info_and_task_metadata_stay_with_the_record() -> None:
    logger, stream = logger_with_output()
    configure_json_logging(logger)
    logger.info(
        "task diagnostic",
        stack_info=True,
        extra={"task_id": "task-123", "task_name": "example.task"},
    )
    lines = stream.getvalue().splitlines()
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert "Stack (most recent call last)" in record["message"]
    assert "test_stack_info_and_task_metadata" in record["message"]
    assert record["metadata"]["task_id"] == "task-123"
    assert record["metadata"]["task_name"] == "example.task"


def test_disabling_fab_silencing_restores_debug_after_a_silenced_app(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = logging.getLogger()
    logger = logging.getLogger("flask_appbuilder")
    stream = io.StringIO()
    monkeypatch.setattr(root, "level", root.level)
    monkeypatch.setattr(logger, "level", logging.ERROR)
    monkeypatch.setattr(logger, "handlers", [logging.StreamHandler(stream)])
    monkeypatch.setattr(logger, "propagate", False)
    config = Config(".")
    config.update(
        SILENCE_FAB=False,
        LOG_LEVEL="DEBUG",
        LOG_FORMAT="%(message)s",
        ENABLE_TIME_ROTATE=False,
    )

    DefaultLoggingConfigurator().configure_logging(config, False)
    logger.debug("permission diagnostic")

    assert stream.getvalue() == "permission diagnostic\n"


@pytest.mark.parametrize("level", ["debug", "info"])
def test_gunicorn_access_logs_honor_the_configured_level(
    level: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from gunicorn.config import Config as GunicornConfig

    from superset.utils.gunicorn_logging import JsonGunicornLogger

    for name in ("gunicorn.error", "gunicorn.access"):
        logger = logging.getLogger(name)
        monkeypatch.setattr(logger, "handlers", [])
        monkeypatch.setattr(logger, "level", logger.level)
        monkeypatch.setattr(logger, "propagate", logger.propagate)
    config = GunicornConfig()
    config.set("loglevel", level)
    config.set("accesslog", "-")
    server_logger = JsonGunicornLogger(config)

    server_logger.access_log.debug("access debug diagnostic")
    server_logger.access_log.info("access request")

    records = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert [record["message"] for record in records] == (
        ["access debug diagnostic", "access request"]
        if level == "debug"
        else ["access request"]
    )
