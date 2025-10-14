import asyncio
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

from src.database.connection import async_session_maker
from src.processing.data_parser import SpimexParser
from src.processing.data_scraper import LinkCollector
from src.processing.db_loader import SpimexLoader


@pytest.fixture
def mock_html():
    return """
    <html>
      <a class="xls" href="/files/oil_xls_20250101120000.xls">01.01.2025</a>
      <a class="xlsx" href="/files/oil_xls_20250520120000.xlsx">20.05.2025</a>
      <a class="xls" href="https://spimex.com/downloads/oil_xls_20250615103045.xls">15.06.2025</a>
    </html>
    """


@pytest.fixture
def queue():
    return asyncio.Queue()


@pytest.fixture
def mock_session(mock_html):
    mock_response = MagicMock(status=200)
    mock_response.text = AsyncMock(return_value=mock_html)

    mock_context = AsyncMock()
    mock_context.__aenter__.return_value = mock_response
    mock_session = MagicMock()
    mock_session.get.return_value = mock_context
    return mock_session


@pytest.fixture
def mock_xls() -> pd.DataFrame:
    xls_files = (
        {
            0: ["", "Дата торгов: 08.10.2025", "", "", ""],
            1: ["", "", "", "", ""],
            2: ["Единица измерения: Метрическая тонна", "", "", "", ""],
            3: ["", "", "", "", ""],
            4: ["", "", "", "", ""],
            5: ["", "0001ABCZ", "Нефть", "База", 10, 100, "", "", "", "", "", "", "", "", 5],
            6: ["", "0002DEFY", "Бензин", "Склад", 20, 200, "", "", "", "", "", "", "", "", 10],
            7: ["", "0003GHIX", "Бензин", "База", 25, 250, "", "", "", "", "", "", "", "", 20],
            8: ["Итого:", "", "", "", "", "", "", "", "", "", "", "", "", "", ""],
        },
        {
            0: ["", "Дата торгов: 09.10.2025", "", "", ""],
            1: ["", "", "", "", ""],
            2: ["Единица измерения: Метрическая тонна", "", "", "", ""],
            3: ["", "", "", "", ""],
            4: ["", "", "", "", ""],
            5: ["", "0001ABCZ", "Нефть", "База", 10, 100, "", "", "", "", "", "", "", "", 5],
            6: ["", "0002DEFY", "Бензин", "Склад", 20, 200, "", "", "", "", "", "", "", "", 10],
            7: ["", "", "", "", "", "", "", "", "", "", "", "", "", "", ""],
            8: ["", "0003GHIX", "Бензин", "База", 25, 250, "", "", "", "", "", "", "", "", 20],
            9: ["Итого:", "", "", "", "", "", "", "", "", "", "", "", "", "", ""],
        },
        {
            0: ["", "Дата торгов: 10.10.2025", "", "", ""],
            1: ["", "", "", "", ""],
            2: ["Единица измерения: Метрическая тонна", "", "", "", ""],
            3: ["", "", "", "", ""],
            4: ["", "", "", "", ""],
            5: ["", "0001ABCZ", "Нефть", "База", 10, 100, "", "", "", "", "", "", "", "", 5],
            6: ["", "0002DEFY", "Бензин", "Склад", 20, 200, "", "", "", "", "", "", "", "", 10],
            7: ["", "0003GHIX", "Бензин", "База", 25, 250, "", "", "", "", "", "", "", "", 20],
            8: ["Итого:", "", "", "", "", "", "", "", "", "", "", "", "", "", ""],
            9: ["", "0004JKLW", "Бензин", "Склад", 30, 350, "", "", "", "", "", "", "", "", 1],
        },
    )
    mock_files = [pd.DataFrame.from_dict(f, orient="index") for f in xls_files]
    return mock_files


@pytest.fixture(autouse=True)
def mock_read_excel(monkeypatch, mock_xls):
    mock_func = MagicMock(side_effect=mock_xls)
    monkeypatch.setattr("src.processing.data_parser.pd.read_excel", mock_func)
    return mock_func


@pytest.fixture
def collector(queue: Any) -> LinkCollector:
    return LinkCollector(start_date=datetime(2025, 1, 1), end_date=datetime(2025, 12, 31), queue=queue)


@pytest.fixture
def parser() -> SpimexParser:
    return SpimexParser(files=["fake1.xls", "fake2.xls", "fake3.xls"])


@pytest.fixture
def loader(parser) -> SpimexLoader:
    parser.parse()
    parsed_df = parser.parsed_df
    return SpimexLoader(sessionmaker=async_session_maker, df=parsed_df, chunk_size=2)


@pytest.mark.asyncio
async def test_extract_links(collector, mock_session):
    links = await collector._extract_links(mock_session, "https://spimex.com/page1")

    assert all(link.startswith("https://spimex.com") for link in links)
    assert all(link.endswith(".xls") for link in links)
    assert len(links) == 2


@pytest.mark.asyncio
async def test_collect_links(monkeypatch, collector, queue):
    mock_data = [
        ["https://spimex.com/a.xls"],
        ["https://spimex.com/b.xls"],
        [],
    ]
    monkeypatch.setattr(collector, "_extract_links", AsyncMock(side_effect=mock_data))

    await collector.collect_links(workers=2)
    results = []
    while not queue.empty():
        results.append(queue.get_nowait())

    assert len([r for r in results if r]) == 2
    assert collector._extract_links.await_count == 3


def test_df_parsing(parser):
    parser.parse()
    parsed_df = parser.parsed_df
    ids = parsed_df["exchange_product_id"].to_list()

    assert not parsed_df.empty
    assert "exchange_product_name" in parsed_df.columns
    assert all(d.year == 2025 for d in parsed_df["date"])
    assert parsed_df["count"].sum() == (5 + 10 + 20) * 3
    assert "0004JKLW" not in ids


@pytest.mark.asyncio
async def test_load_data_to_db(loader):
    await loader.load()
