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

from typing import Optional

from pytest_mock import MockerFixture

from superset.tags.models import Tag
from superset.tasks.warm_monitoring_thumbnails import (
    MONITORING_USER,
    warm_monitoring_thumbnails,
)


def _patch_tag_and_charts(
    mocker: MockerFixture,
    tag: Optional[Tag],
    chart_ids: list[int],
) -> None:
    """Wire ``db.session.query`` to return ``tag`` then one ``TaggedObject`` per id."""
    query = mocker.patch(
        "superset.tasks.warm_monitoring_thumbnails.db.session.query"
    )

    tag_filter = mocker.MagicMock()
    tag_filter.first.return_value = tag

    chart_filter = mocker.MagicMock()
    chart_filter.__iter__.return_value = (
        mocker.MagicMock(object_id=chart_id) for chart_id in chart_ids
    )

    query.return_value.filter.side_effect = [tag_filter, chart_filter]


def test_warm_monitoring_thumbnails_dispatches_as_monitor_user(
    mocker: MockerFixture,
) -> None:
    """
    The warmup must dispatch renders as the ETL user, not ``None``.

    With ``THUMBNAIL_EXECUTORS=[ExecutorType.CURRENT_USER]`` a ``None``
    ``current_user`` makes ``get_executor`` raise ``ExecutorNotFoundError`` in
    ``cache_chart_thumbnail`` (which has no try/except around it), so every
    dispatched render would fail before a screenshot is taken.
    """
    tag = mocker.MagicMock()
    tag.id = 7
    _patch_tag_and_charts(mocker, tag, chart_ids=[101, 102, 103])
    delay = mocker.patch(
        "superset.tasks.warm_monitoring_thumbnails.cache_chart_thumbnail.delay"
    )

    result = warm_monitoring_thumbnails()

    assert result == {"scheduled": 3}
    assert delay.call_count == 3
    for chart_id in (101, 102, 103):
        delay.assert_any_call(
            current_user=MONITORING_USER, chart_id=chart_id, force=True
        )
    # current_user must be a truthy username, never None
    for call in delay.call_args_list:
        assert call.kwargs["current_user"] == MONITORING_USER
        assert call.kwargs["current_user"] is not None


def test_warm_monitoring_thumbnails_schedules_nothing_when_tag_missing(
    mocker: MockerFixture,
) -> None:
    """A missing ``dash_monitoring`` tag short-circuits and schedules nothing."""
    _patch_tag_and_charts(mocker, tag=None, chart_ids=[101])
    delay = mocker.patch(
        "superset.tasks.warm_monitoring_thumbnails.cache_chart_thumbnail.delay"
    )

    result = warm_monitoring_thumbnails()

    assert result == {"scheduled": 0}
    delay.assert_not_called()


def test_warm_monitoring_thumbnails_schedules_nothing_when_no_charts(
    mocker: MockerFixture,
) -> None:
    """A tag with no tagged charts schedules nothing rather than erroring."""
    tag = mocker.MagicMock()
    tag.id = 7
    _patch_tag_and_charts(mocker, tag, chart_ids=[])
    delay = mocker.patch(
        "superset.tasks.warm_monitoring_thumbnails.cache_chart_thumbnail.delay"
    )

    result = warm_monitoring_thumbnails()

    assert result == {"scheduled": 0}
    delay.assert_not_called()

def test_cache_chart_thumbnail_resolves_monitor_user(
    mocker: MockerFixture,
) -> None:
    """
    Prove the fixed ``current_user`` value resolves end-to-end inside
    ``cache_chart_thumbnail`` (no ``ExecutorNotFoundError``) and is used as the
    rendering user. Mirrors the real executor + find_user + override_user chain,
    stubbing only the network/browser screenshot.

    Before the fix, ``current_user=None`` made ``get_executor`` raise and the
    task never reached the screenshot step.
    """
    from superset.tasks.thumbnails import cache_chart_thumbnail
    from superset.tasks.types import ExecutorType

    mocker.patch("superset.tasks.thumbnails.thumbnail_cache", new=True)

    chart = mocker.MagicMock()
    chart.id = 101
    chart.digest = "digest-keyed-to-monitor-user"
    chart.owners = []  # get_executor iterates model.owners unconditionally
    mocker.patch("superset.models.slice.Slice.get", return_value=chart)

    mocker.patch("superset.tasks.thumbnails.get_url_path", return_value="/chart/101")

    monitor_user = mocker.MagicMock()
    monitor_user.username = MONITORING_USER
    mocker.patch(
        "superset.tasks.thumbnails.security_manager.find_user",
        return_value=monitor_user,
    )

    compute = mocker.patch(
        "superset.utils.screenshots.ChartScreenshot.compute_and_cache"
    )

    mocker.patch.dict(
        "flask.current_app.config",
        {"THUMBNAIL_EXECUTORS": [ExecutorType.CURRENT_USER]},
    )

    cache_chart_thumbnail(current_user=MONITORING_USER, chart_id="101", force=True)

    compute.assert_called_once()
    assert compute.call_args.kwargs["user"] is monitor_user
