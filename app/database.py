import os

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from dotenv import load_dotenv

load_dotenv()

CONNECT_TIMEOUT_SECONDS = 5
STATEMENT_TIMEOUT_MS = 5_000
LOCK_TIMEOUT_MS = 2_000
TCP_KEEPALIVES_IDLE_SECONDS = 5
TCP_KEEPALIVES_INTERVAL_SECONDS = 2
TCP_KEEPALIVES_COUNT = 3
TCP_USER_TIMEOUT_MS = 10_000


def get_connection():
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        raise RuntimeError("DATABASE_URL is not configured")

    existing_options = conninfo_to_dict(database_url).get("options", "").strip()
    timeout_options = (
        f"-c statement_timeout={STATEMENT_TIMEOUT_MS} "
        f"-c lock_timeout={LOCK_TIMEOUT_MS}"
    )
    options = " ".join(part for part in (existing_options, timeout_options) if part)

    conninfo = make_conninfo(
        database_url,
        connect_timeout=CONNECT_TIMEOUT_SECONDS,
        keepalives=1,
        keepalives_idle=TCP_KEEPALIVES_IDLE_SECONDS,
        keepalives_interval=TCP_KEEPALIVES_INTERVAL_SECONDS,
        keepalives_count=TCP_KEEPALIVES_COUNT,
        tcp_user_timeout=TCP_USER_TIMEOUT_MS,
        options=options,
    )
    return psycopg.connect(conninfo)
