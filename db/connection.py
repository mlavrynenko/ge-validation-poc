import os
import psycopg2
from contextlib import contextmanager


def get_connection():
    """
    Create and return a new PostgreSQL connection.
    """
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=5432,
        connect_timeout=5,
    )


@contextmanager
def get_db_cursor():
    """
    Context manager that provides a transactional cursor.

    Automatically commits on success,
    rolls back on failure,
    and closes connection safely.
    """
    conn = get_connection()
    cur = conn.cursor()

    try:
        yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
