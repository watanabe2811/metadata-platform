"""bank-conn — logical connection resolver and OpenLineage emitter.

Public API:
    from bank_conn import Connection, configure
"""
from bank_conn.config import configure
from bank_conn.connection import Connection

__version__ = "0.1.0"
__all__ = ["Connection", "configure"]
