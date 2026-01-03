"""DuckDB connection management."""

from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import duckdb

from open_data_stack.config import get_settings


@contextmanager
def get_connection() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Get a DuckDB connection context manager."""
    settings = get_settings()
    conn = duckdb.connect(str(settings.duckdb_path))
    try:
        yield conn
    finally:
        conn.close()


def get_db_path() -> Path:
    """Get the configured database path."""
    return get_settings().duckdb_path
