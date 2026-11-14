
from app.database import get_connection


def test_database_connection():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            result = cur.fetchone()

    assert result[0] == "durableq_test"
