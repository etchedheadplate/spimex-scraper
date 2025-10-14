import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.database.config import DB_HOST, DB_NAME, DB_PASS, DB_PORT, DB_USER
from src.database.models import BaseModel

ASYNC_DATABASE_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


@pytest.fixture(autouse=True)
async def async_engine_fixture():
    engine = create_async_engine(ASYNC_DATABASE_URL, pool_pre_ping=True)
    async with engine.begin() as conn:
        await conn.run_sync(BaseModel.metadata.drop_all)
        await conn.run_sync(BaseModel.metadata.create_all)
    yield engine
    async with engine.begin() as conn:
        await conn.run_sync(BaseModel.metadata.drop_all)
    await engine.dispose()


@pytest.fixture()
async def async_session(async_engine_fixture):
    async_session_maker = async_sessionmaker(bind=async_engine_fixture, expire_on_commit=False)
    async with async_session_maker() as session:
        yield session
