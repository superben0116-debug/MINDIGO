from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
import os
import shutil

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DEFAULT_BUNDLED_DB = os.path.join(BASE_DIR, "ultimate_erp.db")
DEFAULT_DATA_DB = "/data/ultimate_erp.db"


def _resolve_database_url() -> str:
    raw = os.getenv("DATABASE_URL", "").strip()
    if raw:
        return raw
    if os.path.isdir("/data"):
        try:
            if not os.path.exists(DEFAULT_DATA_DB) and os.path.exists(DEFAULT_BUNDLED_DB):
                shutil.copy2(DEFAULT_BUNDLED_DB, DEFAULT_DATA_DB)
        except Exception:
            pass
        return f"sqlite:///{DEFAULT_DATA_DB}"
    return f"sqlite:///{DEFAULT_BUNDLED_DB}"


DATABASE_URL = _resolve_database_url()

connect_args = {"check_same_thread": False, "timeout": 30} if DATABASE_URL.startswith("sqlite") else {}
engine = create_engine(DATABASE_URL, echo=False, connect_args=connect_args)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()
