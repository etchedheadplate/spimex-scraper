import time
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
    chunk_size: int = 5000
    max_parallel_chunks: int = 5


CONFIG = UpdaterConfig()


async def update_database():
    async with async_engine.begin() as conn:
        await conn.run_sync(BaseModel.metadata.create_all)

    try:
        scraper = SpimexScraper(
            CONFIG.date_start, CONFIG.date_end, CONFIG.workers, CONFIG.directory, CONFIG.max_concurrent
        )
        start_scrape = time.perf_counter()
        await scraper.scrape()
        end_scrape = time.perf_counter()
        scrape_time = end_scrape - start_scrape
        files = scraper.scraped_files

        parser = SpimexParser(files)
        start_parse = time.perf_counter()
        parser.parse()
        end_parse = time.perf_counter()
        parse_time = end_parse - start_parse
        parsed_df = parser.parsed_df

        loader = SpimexLoader(
            async_session_maker, parsed_df, CONFIG.update_on_conflict, CONFIG.chunk_size, CONFIG.max_parallel_chunks
        )
        start_load = time.perf_counter()
        await loader.load()
        end_load = time.perf_counter()
        load_time = end_load - start_load

        print(f"[Timer] Скрапинг: {scrape_time:.2f} секунд.")
        print(f"[Timer] Парсинг: {parse_time:.2f} секунд.")
        print(f"[Timer] Загрузка: {load_time:.2f} секунд.")
        print(f"[Timer] Всего: {scrape_time + parse_time + load_time:.2f} секунд.")
    except Exception:
        print("[Updater] Ошибка при обновлении базы данных.")
        return
