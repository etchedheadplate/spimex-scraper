import asyncio
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from src.database.connection import async_session_maker
from src.processing.data_parser import SpimexParser
from src.processing.data_scraper import LinkCollector
from src.processing.db_loader import SpimexLoader


@pytest.fixture
async def db_session():
    async with async_session_maker() as session:
        async with session.begin():
            yield session
            await session.rollback()


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
def collector(queue: Any) -> LinkCollector:
    return LinkCollector(
        start_date=datetime(2025, 1, 1),
        end_date=datetime(2025, 12, 31),
        queue=queue,
    )


@pytest.fixture
def mock_session_with_html(mock_html):
    mock_response = MagicMock(status=200)
    mock_response.text = AsyncMock(return_value=mock_html)

    mock_context = AsyncMock()
    mock_context.__aenter__.return_value = mock_response
    mock_session = MagicMock()
    mock_session.get.return_value = mock_context
    return mock_session


@pytest.fixture(scope="session")
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


@pytest.fixture(scope="session")
@patch("src.processing.data_parser.pd.read_excel")
def parsed_df(mock_read_excel: Any, mock_xls: list[pd.DataFrame]) -> pd.DataFrame:
    mock_read_excel.side_effect = mock_xls

    parser = SpimexParser(files=["fake1.xls", "fake2.xls", "fake3.xls"])
    return parser.parsed_df


@pytest.fixture
def loader(parsed_df):
    return SpimexLoader(sessionmaker=async_session_maker, df=parsed_df, chunk_size=2)


@pytest.mark.asyncio
async def test_extract_links(collector, mock_session_with_html):
    links = await collector._extract_links(mock_session_with_html, "https://spimex.com/page1")

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


def test_create_df(parsed_df):
    result = parsed_df
    ids = result["exchange_product_id"].to_list()

    assert not result.empty
    assert "exchange_product_name" in result.columns
    assert all(d.year == 2025 for d in result["date"])
    assert result["count"].sum() == (5 + 10 + 20) * 3
    assert "0004JKLW" not in ids
