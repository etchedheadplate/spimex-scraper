from dataclasses import dataclass
from datetime import datetime

from src.database.connection import async_engine, async_session_maker
from src.database.models import BaseModel
from src.processing.data_parser import SpimexParser
from src.processing.data_scraper import SpimexScraper
from src.processing.db_loader import SpimexLoader


@dataclass(frozen=True)
class UpdaterConfig:
    date_start: datetime = datetime(2023, 1, 1)
    date_end: datetime = datetime.today()
    directory: str = "bulletins"
    workers: int = 3
    max_concurrent: int = 5
    update_on_conflict: bool = False
    chunk_size: int = 1000


CONFIG = UpdaterConfig()


async def update_database():
    async with async_engine.begin() as conn:
        await conn.run_sync(BaseModel.metadata.create_all)

    async with async_session_maker() as session:
        scraper = SpimexScraper(
            CONFIG.date_start, CONFIG.date_end, CONFIG.workers, CONFIG.directory, CONFIG.max_concurrent
        )
        await scraper.scrape()
        files = scraper.scraped_files

        parser = SpimexParser(files)
        parser.parse()
        parsed_df = parser.parsed_df

        loader = SpimexLoader(session, parsed_df, CONFIG.update_on_conflict, CONFIG.chunk_size)
        await loader.load()
