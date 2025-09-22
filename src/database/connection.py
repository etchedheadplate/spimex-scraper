from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy_utils import create_database, database_exists  # type: ignore

from src.database.config import DB_HOST, DB_NAME, DB_PASS, DB_PORT, DB_USER

SYNC_DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

sync_engine = create_engine(SYNC_DATABASE_URL)

if not database_exists(sync_engine.url):
    create_database(sync_engine.url)
    print(f"[Main] База {DB_NAME} создана.")
else:
    print(f"[Main] База {DB_NAME} уже существует.")

ASYNC_DATABASE_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

async_engine = create_async_engine(ASYNC_DATABASE_URL, pool_pre_ping=True)

async_session_maker = async_sessionmaker(bind=async_engine, expire_on_commit=False)
