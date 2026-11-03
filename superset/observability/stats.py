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
"""Preserve configured legacy stats behavior while adding bounded SDK metrics."""

from superset.observability.metrics import record_legacy
from superset.stats_logger import BaseStatsLogger


class ObservabilityStatsLogger(BaseStatsLogger):
    """A delegating adapter, not a replacement for operator-installed StatsD."""

    def __init__(self, delegate: BaseStatsLogger) -> None:
        super().__init__(getattr(delegate, "prefix", "superset"))
        self.delegate = delegate

    def incr(self, key: str) -> None:
        self.delegate.incr(key)
        record_legacy("counter", key, 1)
        record_legacy("updown", key, 1)

    def decr(self, key: str) -> None:
        self.delegate.decr(key)
        record_legacy("updown", key, -1)

    def timing(self, key: str, value: float) -> None:
        """Preserve legacy milliseconds for delegates; native histograms use s."""
        self.delegate.timing(key, value)
        try:
            record_legacy("histogram", key, value / 1000)
        except (TypeError, ValueError, OverflowError):
            pass

    def gauge(self, key: str, value: float) -> None:
        self.delegate.gauge(key, value)
        record_legacy("gauge", key, value)
