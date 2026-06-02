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
from __future__ import annotations

import json as _json  # noqa: TID251
from typing import Any

JSONDecodeError = _json.JSONDecodeError


def dumps(value: Any, *, sort_keys: bool = False, separators: tuple[str, str] | None = None) -> str:
    """Serialize a value to JSON without importing the Superset app stack."""

    return _json.dumps(value, sort_keys=sort_keys, separators=separators)


def loads(value: str) -> Any:
    """Deserialize a JSON string without importing the Superset app stack."""

    return _json.loads(value)
