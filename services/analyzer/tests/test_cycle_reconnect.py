"""Reconnect behavior tests for cycle()."""

from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock

import pytest

from services.analyzer.worker import cycle


class _TrackingPgPool:
    def __init__(self) -> None:
        self.main = MagicMock(name="main_pg")
        self.worker = MagicMock(name="worker_pg")
        self._available = [self.main, self.worker]
        self.checked_out: list[MagicMock] = []

    def getconn(self) -> MagicMock:
        conn = self._available.pop(0)
        self.checked_out.append(conn)
        return conn

    def putconn(self, conn: MagicMock) -> None:
        self.checked_out.remove(conn)
        self._available.append(conn)


class _TrackingSrPool:
    def __init__(self) -> None:
        self.conn = MagicMock(name="sr")

    def get_connection(self) -> MagicMock:
        return self.conn


def _mysql_errors(monkeypatch: pytest.MonkeyPatch):
    try:
        from mysql.connector import errors as mysql_errors

        return mysql_errors
    except ImportError:
        pass

    mysql = types.ModuleType("mysql")
    connector = types.ModuleType("mysql.connector")
    errors = types.ModuleType("mysql.connector.errors")

    class Error(Exception):
        pass

    class OperationalError(Error):
        pass

    class InterfaceError(Error):
        pass

    errors.Error = Error
    errors.OperationalError = OperationalError
    errors.InterfaceError = InterfaceError
    connector.errors = errors
    mysql.connector = connector

    monkeypatch.setitem(sys.modules, "mysql", mysql)
    monkeypatch.setitem(sys.modules, "mysql.connector", connector)
    monkeypatch.setitem(sys.modules, "mysql.connector.errors", errors)
    return errors


def _target() -> list[tuple[str, str | None, None]]:
    return [("player1", "game1", None)]


def test_cycle_propagates_starrocks_operational_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mysql_errors = _mysql_errors(monkeypatch)
    pg_pool = _TrackingPgPool()
    sr_pool = _TrackingSrPool()

    monkeypatch.setattr("services.analyzer.worker.fetch_eligible_players", lambda _pg, _limit: _target())

    def fail(*_args: object) -> int:
        raise mysql_errors.OperationalError("torn")

    monkeypatch.setattr("services.analyzer.worker.process_player", fail)

    with pytest.raises(mysql_errors.OperationalError):
        cycle(pg_pool, sr_pool)

    assert pg_pool.checked_out == []
    pg_pool.worker.rollback.assert_called_once()
    sr_pool.conn.close.assert_called_once()


def test_cycle_swallows_business_logic_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    pg_pool = _TrackingPgPool()
    sr_pool = _TrackingSrPool()

    monkeypatch.setattr("services.analyzer.worker.fetch_eligible_players", lambda _pg, _limit: _target())

    def fail(*_args: object) -> int:
        raise ValueError("bad data")

    monkeypatch.setattr("services.analyzer.worker.process_player", fail)

    assert cycle(pg_pool, sr_pool) == 1
    assert pg_pool.checked_out == []
    pg_pool.worker.rollback.assert_called_once()
    sr_pool.conn.close.assert_called_once()
