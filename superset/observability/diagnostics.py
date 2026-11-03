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
"""Process-local diagnostics that never invoke logging, SDKs, or network I/O."""

import os
import threading

_lock = threading.Lock()
_failures = 0


def record_failure() -> None:
    """Count a failed instrumentation operation without recursively observing it."""
    global _failures
    with _lock:
        _failures += 1


def failures() -> int:
    """Read the cumulative count for the current process writer."""
    with _lock:
        return _failures


def _after_fork() -> None:
    global _lock, _failures
    _lock = threading.Lock()
    _failures = 0


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_after_fork)
