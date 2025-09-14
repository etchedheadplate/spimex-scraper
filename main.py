import asyncio
import os
from datetime import datetime

from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.loader import SpimexLoader
from src.models import BaseModel
from src.parser import SpimexParser
from src.scraper import SpimexScraper

load_dotenv()

DB_NAME = os.environ.get("DB_NAME")
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT")
DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASS")

DATABASE_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)

AsyncSessionLocal = async_sessionmaker(bind=engine, expire_on_commit=False)


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(BaseModel.metadata.create_all)

    async with AsyncSessionLocal() as session:
        start = datetime(2023, 1, 1)
        end = datetime.today()

        scraper = SpimexScraper(start, end)
        await scraper.scrape()
        files = scraper.scraped_files

        parser = SpimexParser(files)
        parser.parse()
        parsed_df = parser.parsed_df

        loader = SpimexLoader(session, parsed_df)
        await loader.load(update_on_conflict=False)


if __name__ == "__main__":
    asyncio.run(main())
