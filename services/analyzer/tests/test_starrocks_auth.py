"""Tests for StarRocks auth fallback helpers."""

from __future__ import annotations

from types import SimpleNamespace

from services.analyzer.starrocks import (
    connect_with_passwordless_fallback,
    pool_with_passwordless_fallback,
)


class AccessDenied(Exception):
    errno = 1045


def test_connect_retries_passwordless_on_access_denied() -> None:
    calls: list[dict] = []

    def connect(**kwargs):
        calls.append(kwargs)
        if kwargs.get("password"):
            raise AccessDenied("Access denied")
        return "connected"

    mysql_connector = SimpleNamespace(connect=connect)

    assert connect_with_passwordless_fallback(
        mysql_connector,
        host="starrocks-fe",
        port=9030,
        user="root",
        password="stale",
    ) == "connected"

    assert [call.get("password") for call in calls] == ["stale", ""]


def test_pool_retries_passwordless_on_access_denied() -> None:
    calls: list[dict] = []

    class Pooling:
        @staticmethod
        def MySQLConnectionPool(**kwargs):
            calls.append(kwargs)
            if kwargs.get("password"):
                raise AccessDenied("Access denied")
            return "pool"

    assert pool_with_passwordless_fallback(
        Pooling,
        pool_name="analyzer",
        pool_size=6,
        host="starrocks-fe",
        port=9030,
        user="root",
        password="stale",
    ) == "pool"

    assert [call.get("password") for call in calls] == ["stale", ""]


def test_connect_uses_configured_password_when_valid() -> None:
    calls: list[dict] = []

    def connect(**kwargs):
        calls.append(kwargs)
        return "connected"

    mysql_connector = SimpleNamespace(connect=connect)

    assert connect_with_passwordless_fallback(
        mysql_connector,
        host="starrocks-fe",
        port=9030,
        user="root",
        password="valid",
    ) == "connected"

    assert [call.get("password") for call in calls] == ["valid"]
