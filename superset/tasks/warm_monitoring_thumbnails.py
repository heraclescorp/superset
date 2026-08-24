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

"""Warm dash_monitoring chart screenshots before the 14:00 ETL."""

from superset import db
from superset.extensions import celery_app
from superset.tags.models import Tag, TaggedObject
from superset.tasks.thumbnails import cache_chart_thumbnail

MONITORING_TAG = "dash_monitoring"


@celery_app.task(name="warm_monitoring_thumbnails")
def warm_monitoring_thumbnails() -> dict[str, int]:
    tag = db.session.query(Tag).filter(Tag.name == MONITORING_TAG).first()
    if tag is None:
        return {"scheduled": 0}

    chart_ids = [
        tagged.object_id
        for tagged in db.session.query(TaggedObject).filter(
            TaggedObject.object_type == "chart",
            TaggedObject.tag_id == tag.id,
        )
    ]

    for chart_id in chart_ids:
        # current_user None -> executor falls back to the configured Selenium user
        cache_chart_thumbnail.delay(current_user=None, chart_id=chart_id, force=True)

    return {"scheduled": len(chart_ids)}
