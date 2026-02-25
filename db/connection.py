from contextlib import contextmanager

import psycopg2

from core.settings import load_settings


def get_connection():
    """
    Create and return a new PostgreSQL connection.
    Sets search_path so unqualified table names resolve correctly.
    """
    settings = load_settings()

    conn = psycopg2.connect(
        host=settings.DB_HOST,
        database=settings.DB_NAME,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
        port=settings.DB_PORT,
        connect_timeout=5,
    )

    # Ensure dq schema is used by default
    with conn.cursor() as cur:
        cur.execute("SET search_path TO dq, public")

    return conn


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
