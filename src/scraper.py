import asyncio
import os
import re
from datetime import datetime
from urllib.parse import urljoin

import aiofiles
import aiohttp
from bs4 import BeautifulSoup, Tag


class LinkCollector:
    def __init__(self, start_date: datetime, end_date: datetime, queue: asyncio.Queue[str | None]) -> None:
        self.start_date = start_date
        self.end_date = end_date
        self.base_url = "https://spimex.com"
        self.start_page = f"{self.base_url}/markets/oil_products/trades/results/"
        self.queue = queue

    async def _extract_links(self, session: aiohttp.ClientSession, url: str) -> list[str]:
        print(f"[Collector] Загружаю страницу: {url}")
        try:
            async with session.get(url) as resp:
                if resp.status != 200:
                    print(f"[Collector] Ошибка {resp.status} при загрузке {url}")
                    return []
                text = await resp.text()
        except Exception as e:
            print(f"[Collector] Ошибка при запросе {url}: {e}")
            return []

        soup = BeautifulSoup(text, "html.parser")
        links: list[str] = []

        for link in soup.find_all("a", class_="xls"):
            if isinstance(link, Tag):
                href = link.get("href")
                if isinstance(href, str) and "oil_xls_" in href:
                    match = re.search(r"oil_xls_(\d{14})\.xls", href)
                    if not match:
                        continue
                    timestamp_str = match.group(1)
                    file_date = datetime.strptime(timestamp_str, "%Y%m%d%H%M%S")
                    if self.start_date <= file_date <= self.end_date:
                        full_url = urljoin(self.base_url, href)
                        print(f"[Collector] Найдена ссылка: {full_url}")
                        links.append(full_url)
        return links

    async def collect_links(self, workers: int) -> None:
        page = 1
        count = 0

        async with aiohttp.ClientSession() as session:
            while True:
                url = self.start_page + (f"?page=page-{page}" if page > 1 else "")
                page_links = await self._extract_links(session, url)
                if not page_links:
                    print(f"[Collector] На странице {page} ссылки не найдены. Остановка.")
                    break

                for link in page_links:
                    await self.queue.put(link)
                    count += 1
                    print(f"[Collector] Ссылка добавлена в очередь: {link}")

                page += 1

        for _ in range(workers):
            await self.queue.put(None)
        print(f"[Collector] Всего добавлено {count} ссылок. В очередь отправлены сигналы завершения.")


class FileDownloader:
    def __init__(
        self,
        download_dir: str = "bulletins",
        max_concurrent: int = 5,
        queue: asyncio.Queue[str | None] | None = None,
    ) -> None:
        self.download_dir = download_dir
        self.max_concurrent = max_concurrent
        self.queue = queue or asyncio.Queue()
        os.makedirs(download_dir, exist_ok=True)
        self.downloaded_files: list[str] = []

    async def _download_file(self, session: aiohttp.ClientSession, url: str) -> None:
        filename = url.split("/")[-1].split("?")[0]
        filepath = os.path.join(self.download_dir, filename)

        if os.path.exists(filepath):
            print(f"[Downloader] Файл уже существует: {filepath}, пропускаем.")
            return

        print(f"[Downloader] Начинаю скачивание: {url}")
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    async with aiofiles.open(filepath, "wb") as f:
                        async for chunk in resp.content.iter_chunked(8192):
                            await f.write(chunk)
                    print(f"[Downloader] Успешно скачан файл: {filepath}")
                    self.downloaded_files.append(filepath)
                else:
                    print(f"[Downloader] Ошибка {resp.status} при скачивании {url}")
        except Exception as e:
            print(f"[Downloader] Ошибка при скачивании {url}: {e}")

    async def consume_queue(self, worker_id: int = 1) -> None:
        sem = asyncio.Semaphore(self.max_concurrent)
        async with aiohttp.ClientSession() as session:
            while True:
                url = await self.queue.get()
                if url is None:
                    print(f"[Downloader-{worker_id}] Получен сигнал завершения. Остановка.")
                    self.queue.task_done()
                    break

                print(f"[Downloader-{worker_id}] Взял из очереди: {url}")
                async with sem:
                    await self._download_file(session, url)

                self.queue.task_done()
                print(f"[Downloader-{worker_id}] Завершил обработку: {url}")


class SpimexScraper:
    def __init__(self, start_date: datetime, end_date: datetime, workers: int = 3) -> None:
        self.queue: asyncio.Queue[str | None] = asyncio.Queue()
        self.collector = LinkCollector(start_date, end_date, self.queue)
        self.downloader = FileDownloader(queue=self.queue)
        self.workers = workers
        self.scraped_files: list[str] = []

    async def scrape(self) -> None:
        print("[Scraper] Запуск producer (сбор ссылок) и consumers (скачивание).")

        producer = asyncio.create_task(self.collector.collect_links(self.workers))
        consumers = [
            asyncio.create_task(self.downloader.consume_queue(worker_id=i)) for i in range(1, self.workers + 1)
        ]

        await asyncio.gather(producer, *consumers)

        self.scraped_files = self.downloader.downloaded_files.copy()
        print(f"[Scraper] Всего загружено {len(self.scraped_files)} файлов.")
        print("[Scraper] Все задачи завершены.")
