import asyncio
import os
import re
from datetime import datetime
from urllib.parse import urljoin

import aiofiles
import aiohttp
import requests
from bs4 import BeautifulSoup, Tag


class LinkExtractor:
    def __init__(self, start_date: datetime, end_date: datetime) -> None:
        self.start_date = start_date
        self.end_date = end_date
        self.base_url = "https://spimex.com"
        self.start_page = f"{self.base_url}/markets/oil_products/trades/results/"

    def _extract_links(self, url: str) -> list[str]:
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            return []

        soup = BeautifulSoup(response.text, "html.parser")
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
                        links.append(full_url)
        return links

    def collect_links(self) -> list[str]:
        all_links: list[str] = []
        page = 1

        while True:
            url = self.start_page + (f"?page=page-{page}" if page > 1 else "")
            page_links = self._extract_links(url)
            if not page_links:
                break
            all_links.extend(page_links)
            page += 1

        return all_links


class FileDownloader:
    def __init__(self, download_dir: str = "bulletins", max_concurrent: int = 5) -> None:
        self.download_dir = download_dir
        self.max_concurrent = max_concurrent
        os.makedirs(download_dir, exist_ok=True)

    async def _download_file(self, session: aiohttp.ClientSession, url: str, sem: asyncio.Semaphore) -> None:
        filename = url.split("/")[-1].split("?")[0]
        filepath = os.path.join(self.download_dir, filename)

        async with sem:
            try:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        async with aiofiles.open(filepath, "wb") as f:
                            async for chunk in resp.content.iter_chunked(8192):
                                await f.write(chunk)
                    else:
                        print(f"Ошибка {resp.status} при скачивании {url}")
            except Exception as e:
                print(f"Ошибка при скачивании {url}: {e}")

    async def download_all(self, urls: list[str]) -> None:
        sem = asyncio.Semaphore(self.max_concurrent)
        async with aiohttp.ClientSession() as session:
            tasks = [self._download_file(session, url, sem) for url in urls]
            await asyncio.gather(*tasks)


class SpimexScraper:
    def __init__(self, start_date: datetime, end_date: datetime) -> None:
        self.scraper = LinkExtractor(start_date, end_date)
        self.downloader = FileDownloader()

    def run(self) -> None:
        links = self.scraper.collect_links()
        if not links:
            print("Файлов для скачивания не найдено.")
            return
        print(f"Найдено {len(links)} файлов для скачивания.")
        asyncio.run(self.downloader.download_all(links))


if __name__ == "__main__":
    start = datetime(2023, 1, 1)
    end = datetime.today()
    SpimexScraper(start, end).run()
