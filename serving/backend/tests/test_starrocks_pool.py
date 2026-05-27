from mysql.connector import errors

from serving.backend import db


class FakeCursor:
    def __init__(self):
        self.sql = None
        self.params = None

    def execute(self, sql, params=()):
        self.sql = sql
        self.params = params

    def fetchall(self):
        return [{"ok": 1}]

    def close(self):
        pass


class FakeConnection:
    def __init__(self):
        self.closed = False

    def ping(self, reconnect=True, attempts=2, delay=0):
        pass

    def cursor(self, dictionary=True):
        return FakeCursor()

    def close(self):
        self.closed = True


class FlakyPool:
    def __init__(self, failures):
        self.failures = failures
        self.calls = 0
        self.connection = FakeConnection()

    def get_connection(self):
        self.calls += 1
        if self.calls <= self.failures:
            raise errors.PoolError("pool exhausted")
        return self.connection


def test_starrocks_cursor_waits_when_pool_is_temporarily_exhausted(monkeypatch):
    pool = FlakyPool(failures=2)
    monkeypatch.setattr(db.StarRocks, "_pool", pool)
    monkeypatch.setattr(db, "STARROCKS_POOL_WAIT_SECONDS", 1.0)
    monkeypatch.setattr(db.time, "sleep", lambda _seconds: None)

    assert db._run("SELECT 1") == [{"ok": 1}]
    assert pool.calls == 3
    assert pool.connection.closed is True
