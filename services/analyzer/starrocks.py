"""StarRocks connection helpers shared by analyzer entrypoints."""

from __future__ import annotations

import logging
from typing import Any


log = logging.getLogger(__name__)


def _is_access_denied(exc: BaseException) -> bool:
    errno = getattr(exc, "errno", None)
    if errno == 1045:
        return True
    return "Access denied" in str(exc)


def _without_password(kwargs: dict[str, Any]) -> dict[str, Any]:
    fallback = dict(kwargs)
    fallback["password"] = ""
    return fallback


def connect_with_passwordless_fallback(mysql_connector: Any, **kwargs: Any) -> Any:
    """Connect to StarRocks with configured password, then passwordless root.

    StarRocks FE can come back passwordless after state resets/restarts while
    Kubernetes still holds an older STARROCKS_PASSWORD. The analyzer should not
    crash-loop in that condition.
    """
    password = kwargs.get("password") or ""
    if not password:
        return mysql_connector.connect(**kwargs)

    try:
        return mysql_connector.connect(**kwargs)
    except Exception as exc:
        if not _is_access_denied(exc):
            raise
        log.warning("starrocks rejected configured password; retrying passwordless auth")
        return mysql_connector.connect(**_without_password(kwargs))


def pool_with_passwordless_fallback(
    mysql_pooling: Any,
    *,
    pool_name: str,
    pool_size: int,
    **kwargs: Any,
) -> Any:
    """Create a StarRocks MySQL pool with passwordless fallback on auth drift."""
    password = kwargs.get("password") or ""
    if not password:
        return mysql_pooling.MySQLConnectionPool(
            pool_name=pool_name,
            pool_size=pool_size,
            **kwargs,
        )

    try:
        return mysql_pooling.MySQLConnectionPool(
            pool_name=pool_name,
            pool_size=pool_size,
            **kwargs,
        )
    except Exception as exc:
        if not _is_access_denied(exc):
            raise
        log.warning("starrocks rejected configured password; creating passwordless pool")
        return mysql_pooling.MySQLConnectionPool(
            pool_name=pool_name,
            pool_size=pool_size,
            **_without_password(kwargs),
        )
