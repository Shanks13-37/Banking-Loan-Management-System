from __future__ import annotations

import sqlite3
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
DATABASE_DIR = ROOT_DIR / "database"
DEFAULT_DB_PATH = DATABASE_DIR / "banking.db"
SCHEMA_PATH = DATABASE_DIR / "schema.sql"
SEED_PATH = DATABASE_DIR / "seed.sql"


def _sqlite_sidecar_paths(database_path: Path) -> tuple[Path, Path]:
    return (
        database_path.with_name(f"{database_path.name}-wal"),
        database_path.with_name(f"{database_path.name}-shm"),
    )


def get_connection(db_path: Path | str = DEFAULT_DB_PATH, *, autocommit: bool = False) -> sqlite3.Connection:
    database_path = Path(db_path)
    database_path.parent.mkdir(parents=True, exist_ok=True)

    isolation_level = None if autocommit else "DEFERRED"
    connection = sqlite3.connect(
        database_path,
        timeout=30.0,
        isolation_level=isolation_level,
        check_same_thread=False,
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA busy_timeout = 30000")
    connection.execute("PRAGMA journal_mode = WAL")
    return connection


def init_db(db_path: Path | str = DEFAULT_DB_PATH, *, reset: bool = False) -> Path:
    database_path = Path(db_path)
    database_path.parent.mkdir(parents=True, exist_ok=True)

    if reset:
        for path in (database_path, *_sqlite_sidecar_paths(database_path)):
            if path.exists():
                path.unlink()

    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
    seed_sql = SEED_PATH.read_text(encoding="utf-8")

    with get_connection(database_path, autocommit=True) as connection:
        connection.executescript(schema_sql)
        connection.executescript(seed_sql)

    return database_path
