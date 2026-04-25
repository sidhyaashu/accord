import time
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from app.config import DATABASE_URL


def build_engine() -> Engine:
    return create_engine(
        DATABASE_URL,
        echo=False,
        future=True,
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_size=2,
        max_overflow=0,
        connect_args={"connect_timeout": 10},
    )


def wait_for_db(engine: Engine, retries: int = 20, delay: int = 3) -> None:
    for attempt in range(1, retries + 1):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("✅ Database connection successful")
            return
        except Exception as e:
            print(f"⏳ Waiting for DB... {attempt}/{retries} | {e}")
            time.sleep(delay)

    raise RuntimeError("Database connection failed")