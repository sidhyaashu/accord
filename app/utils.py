from sqlalchemy import inspect
from sqlalchemy.engine import Engine

def resolve_table_name(engine: Engine, feed_name: str) -> str | None:
    inspector = inspect(engine)
    db_tables = inspector.get_table_names()

    return next((t for t in db_tables if t.lower() == feed_name.lower()), None)
