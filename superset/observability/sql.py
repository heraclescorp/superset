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
"""Application-observed physical SQL, without SQL text or result telemetry.

Execute and fetch measure client wall time, not Snowflake's queue/execution time.
Each attempted statement gets an independent identifier before reaching the driver.
Native query IDs are captured before the cursor can be reused; cancellation handles
are deliberately never consulted. QUERY_TAG uses request-local connector statement
parameters, so pooled sessions retain their operator-configured tags and transaction
state.
"""

from __future__ import annotations

import hashlib
import re
from contextlib import contextmanager
from typing import Any, Iterator, TYPE_CHECKING
from uuid import uuid4

from sqlglot.dialects import Dialect

from superset.observability import get_context, is_enabled, operation, telemetry_context
from superset.sql.parse import SQLGLOT_DIALECTS
from superset.utils import json

if TYPE_CHECKING:
    import pandas as pd

    from superset.models.core import Database

_TAG_KEYS = (
    "statement_id",
    "source",
    "statement_index",
    "request_id",
    "execution_id",
    "task_id",
    "celery_task_id",
    "sql_lab_query_id",
    "sql_lab_client_id",
    "chart_id",
    "dashboard_id",
    "dataset_id",
    "database_id",
    "request_actor_user_id",
    "effective_user_id",
    "guest_actor_id",
    "report_schedule_id",
    "report_execution_id",
    "warmup_run_id",
    "query_index",
    "force_refresh",
    "async_execution",
    "sql_fingerprint",
)
_NATIVE_ID = re.compile(r"[A-Za-z0-9_-]{1,128}\Z")


def _statement_type(tokens: list[Any]) -> str:  # noqa: C901
    """Classify bounded workload kinds, including the outer command of a CTE."""
    if not tokens:
        return "other"
    command = tokens[0].text.upper()
    if command == "WITH":
        depth = 0
        for token in tokens:
            kind = token.token_type.name
            if kind == "L_PAREN":
                depth += 1
            elif kind == "R_PAREN":
                depth -= 1
            elif depth == 0 and kind in {
                "SELECT",
                "INSERT",
                "UPDATE",
                "DELETE",
                "MERGE",
            }:
                command = kind
                break
    if command == "SELECT":
        return "select"
    if command in {"SET", "UNSET", "USE", "BEGIN", "COMMIT", "ROLLBACK"} or (
        command == "ALTER" and len(tokens) > 1 and tokens[1].text.upper() == "SESSION"
    ):
        return "session"
    if command in {"SHOW", "DESCRIBE", "DESC", "EXPLAIN"}:
        return "metadata"
    if command in {"INSERT", "UPDATE", "DELETE", "MERGE", "REPLACE", "UPSERT", "COPY"}:
        return "dml"
    if command in {
        "CREATE",
        "ALTER",
        "DROP",
        "TRUNCATE",
        "RENAME",
        "COMMENT",
        "GRANT",
        "REVOKE",
    }:
        return "ddl"
    return "other"


def _sql_signature(sql: str, engine: str) -> tuple[str | None, str]:
    """Hash a bounded, comment-free token stream with literals/binds normalized.

    Tokenization avoids parser fallback warnings that can contain SQL. Unsupported
    syntax is allowed to execute unchanged and simply has no fingerprint. Only the
    digest leaves this function, never the token stream or a tokenizer exception.
    """
    if not is_enabled() or len(sql) > 1_000_000:
        return None, "other"
    try:
        dialect = Dialect.get_or_raise(SQLGLOT_DIALECTS.get(engine))
        tokens = dialect.tokenize(sql)
        digest = hashlib.sha256()
        bind_name = False
        for token in tokens:
            kind = token.token_type.name
            if kind == "SEMICOLON":
                continue
            if bind_name or kind == "NUMBER" or "STRING" in kind:
                value = "?"
            elif kind in {"PARAMETER", "SESSION_PARAMETER", "PLACEHOLDER"}:
                value = "?"
            else:
                value = token.text if kind == "IDENTIFIER" else token.text.upper()
            digest.update(value.encode("utf-8"))
            digest.update(b"\x00")
            bind_name = kind in {"COLON", "PARAMETER", "SESSION_PARAMETER"}
        return digest.hexdigest(), _statement_type(tokens)
    except Exception:  # noqa: BLE001 -- telemetry must not reject executable SQL
        return None, "other"


def sql_fingerprint(sql: str, engine: str) -> str | None:
    """Return only the normalized digest; no SQL/token data crosses this boundary."""
    return _sql_signature(sql, engine)[0]


def statement_query_tag() -> str | None:
    """Return the versioned bounded tag for the active physical statement only.

    Connector 4.8.0 accepts ``_statement_params`` and sends QUERY_TAG on that
    request, without ALTER SESSION or a persistent session-parameter mutation.
    Values already pass Foundation's allowlist; this boundary independently
    rejects objects and oversized strings rather than serializing arbitrary data.
    """
    if not is_enabled():
        return None
    context = get_context()
    if not isinstance(context.get("statement_id"), str):
        return None
    tag: dict[str, str | int | bool] = {"superset_observability": 1}
    for key in _TAG_KEYS:
        value = context.get(key)
        if type(value) not in {str, int, bool}:
            continue
        if isinstance(value, str) and len(value) > 128:
            continue
        if type(value) is int and not -(2**63) <= value < 2**64:
            continue
        candidate = {**tag, key: value}
        if len(json.dumps(candidate, separators=(",", ":")).encode("utf-8")) <= 1800:
            tag[key] = value
    return json.dumps(tag, separators=(",", ":"))


def execute_with_statement_tag(cursor: Any, sql: str) -> Any:
    """Execute through the pinned Snowflake API without mutating session state."""
    tag = statement_query_tag()
    if tag is None:
        return cursor.execute(sql)
    return cursor.execute(sql, _statement_params={"QUERY_TAG": tag})


def dataframe_attributes(df: pd.DataFrame) -> dict[str, int]:
    """Report rows and estimated native dataframe storage, never its values.

    Bytes exclude recursively referenced Python objects and serialization overhead;
    avoiding a deep traversal keeps measurement independent of result contents.
    """
    if not is_enabled():
        return {}
    try:
        return {
            "rows": len(df.index),
            "result_bytes": int(df.memory_usage(index=True, deep=False).sum()),
        }
    except Exception:  # noqa: BLE001
        return {}


def _native_id(value: Any) -> str | None:
    """Accept a bounded native driver identifier without stringifying objects."""
    return value if isinstance(value, str) and _NATIVE_ID.fullmatch(value) else None


def _read_attribute(value: Any, name: str) -> Any:
    """Read optional driver state without letting a property break execution."""
    try:
        return getattr(value, name, None)
    except Exception:  # noqa: BLE001
        return None


class StatementObservation:
    """One physical attempt, with separate execute and fetch measurements."""

    def __init__(self, cursor: Any, parent: Any, db_system: str) -> None:
        self.cursor: Any = None
        self.parent = parent
        self.db_system = db_system
        self.native: dict[str, str] = {}
        self.previous_query_id: str | None = None
        self.set_cursor(cursor)

    def set_cursor(self, cursor: Any) -> None:
        """Attach the SQLAlchemy-created cursor before its physical execution."""
        self.cursor = cursor
        if self.db_system != "snowflake" or cursor is None:
            return
        self.previous_query_id = _native_id(_read_attribute(cursor, "sfqid"))
        session_id = _read_attribute(
            _read_attribute(cursor, "connection"), "session_id"
        )
        if type(session_id) is int and not 0 <= session_id < 2**64:
            return
        if type(session_id) not in {str, int}:
            return
        session_id = str(session_id)
        if _NATIVE_ID.fullmatch(session_id):
            self.native["snowflake_session_id"] = session_id
            self.parent.set_attribute("snowflake_session_id", session_id)

    def capture(self, target: Any, error: BaseException | None = None) -> None:  # noqa: C901
        """Snapshot the fresh sfqid, including IDs attached to driver failures."""
        if not is_enabled():
            return
        error_code = None
        query_id = None
        seen: set[int] = set()
        cause = error
        while cause is not None and id(cause) not in seen and len(seen) < 8:
            seen.add(id(cause))
            if error_code is None:
                for key in ("sqlstate", "pgcode", "errno"):
                    value = _read_attribute(cause, key)
                    if type(value) is int or (
                        isinstance(value, str)
                        and 0 < len(value) <= 16
                        and value.isalnum()
                    ):
                        error_code = str(value)
                        break
            candidate = _native_id(_read_attribute(cause, "sfqid"))
            if query_id is None and candidate and candidate != self.previous_query_id:
                query_id = candidate
            original = _read_attribute(cause, "orig")
            cause = (
                original
                if isinstance(original, BaseException)
                else _read_attribute(cause, "__cause__")
            )
        if error_code is not None:
            self.parent.set_attribute("error_code", error_code)
            target.set_attribute("error_code", error_code)
        if self.db_system != "snowflake":
            return
        if query_id is None:
            candidate = _native_id(_read_attribute(self.cursor, "sfqid"))
            if candidate != self.previous_query_id:
                query_id = candidate
        if query_id:
            self.native["snowflake_query_id"] = query_id
        for key, value in self.native.items():
            self.parent.set_attribute(key, value)
            target.set_attribute(key, value)

    @contextmanager
    def _phase(self, name: str) -> Iterator[Any]:
        with operation(name, **self.native) as observed:
            try:
                yield observed
            except BaseException as error:
                self.capture(observed, error)
                raise
            else:
                self.capture(observed)

    def execute(self) -> Any:
        """Measure only the driver's execute call, preserving its exception."""
        return self._phase("sql.execute")

    def fetch(self) -> Any:
        """Measure the driver's result retrieval, preserving its exception."""
        return self._phase("sql.fetch")


def execute_tagged_sqlalchemy(
    cursor: Any, sql: str, parameters: Any, context: Any
) -> bool | None:
    """Snowflake dialect hook for explicitly observed SQLAlchemy statements.

    Registered before an engine is published to the pool cache. Unrelated
    SQLAlchemy work retains the dialect's default execution path; streaming
    exports keep SQLAlchemy's original result conversion and exception handling.
    """
    if not is_enabled():
        return None
    observed = context.execution_options.get("superset_statement_observation")
    if not isinstance(observed, StatementObservation):
        return None
    observed.set_cursor(cursor)
    tag = statement_query_tag()
    if tag is None:
        return None
    cursor.execute(sql, parameters, _statement_params={"QUERY_TAG": tag})
    return True


@contextmanager
def observe_statement(
    database: Database,
    cursor: Any,
    sql: str,
    *,
    statement_index: int = 0,
    **attributes: Any,
) -> Iterator[StatementObservation]:
    """Create safe attempt context; never attach SQL/binds/raw errors to telemetry."""
    if not is_enabled():
        with operation("sql.statement") as observed:
            yield StatementObservation(None, observed, "")
        return
    db_system = database.db_engine_spec.engine
    fingerprint, statement_type = _sql_signature(sql, db_system)
    context = {
        "source": get_context().get("source") or "metadata",
        **attributes,
        "database_id": database.id,
        "db_system": db_system,
        "statement_id": str(uuid4()),
        "statement_index": statement_index,
        "sql_fingerprint": fingerprint,
        "statement_type": statement_type,
        # A pooled cursor or inherited task context must not carry the last query ID.
        "snowflake_query_id": None,
        "snowflake_session_id": None,
    }
    if context["source"] == "metadata":
        context["statement_type"] = "metadata"
    with telemetry_context(**context), operation("sql.statement") as observed:
        statement = StatementObservation(cursor, observed, db_system)
        try:
            yield statement
        except BaseException as error:
            statement.capture(observed, error)
            raise
        else:
            statement.capture(observed)
