import asyncio
import random
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest
from faker import Faker

from src.database.connection import async_session_maker
from src.processing.data_parser import SpimexParser
from src.processing.data_scraper import LinkCollector
from src.processing.db_loader import SpimexLoader

fake = Faker()

FILES_COUNT = 10
BULLETIN_DATE = [fake.date_between(start_date="-1m", end_date="today") for _ in range(100)]
EXCHANGE_PRODUCT_IDS = [f"OIL{i}" for i in range(1, 6)]
DELIVERY_BASIS_IDS = [f"DB{i}" for i in range(1, 6)]
DELIVERY_BASIS_NAMES = [f"Basis{i}" for i in range(1, 6)]
DELIVERY_TYPE_IDS = ["A", "B", "C", "D", "E"]


def generate_product_name():
    part1 = random.choice(EXCHANGE_PRODUCT_IDS)
    part2 = random.choice(DELIVERY_BASIS_IDS)
    part3 = random.choice(DELIVERY_TYPE_IDS)
    return f"{part1}{part2}{part3}"


def generate_mock_xls_with_dates(num_lines: int = 4) -> list[pd.DataFrame]:
    dfs = []
    for _ in range(FILES_COUNT):
        date_obj = random.choice(BULLETIN_DATE)
        date_str_formatted = date_obj.strftime("%d.%m.%Y")

        data = {
            0: ["", f"Дата торгов: {date_str_formatted}", "", "", ""],
            1: ["", "", "", "", ""],
            2: ["Единица измерения: Метрическая тонна", "", "", "", ""],
            3: ["", "", "", "", ""],
            4: ["", "", "", "", ""],
        }

        for i in range(num_lines):
            count = random.randint(1, 50)
            volume = count * random.randint(1, 10)
            total = volume * random.randint(10, 50)
            exchange_product_id = generate_product_name()
            delivery_basis_name = random.choice(DELIVERY_BASIS_NAMES)

            data[5 + i] = [
                "",
                exchange_product_id,
                "",
                delivery_basis_name,
                volume,
                total,
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                count,
            ]

        data[5 + num_lines] = ["Итого:"] + [""] * 14
        dfs.append(pd.DataFrame.from_dict(data, orient="index"))

    return dfs


@pytest.fixture
def mock_html() -> str:
    return """
    <html>
      <a class="xls" href="/files/oil_xls_20250101120000.xls">01.03.2025</a>
      <a class="xlsx" href="/files/oil_xls_20250520120000.xlsx">20.05.2025</a>
      <a class="xls" href="https://spimex.com/downloads/oil_xls_20250615103045.xls">15.06.2025</a>
    </html>
    """


@pytest.fixture
def queue() -> asyncio.Queue:
    return asyncio.Queue()


@pytest.fixture
def mock_session(mock_html: str):
    mock_response = MagicMock(status=200)
    mock_response.text = AsyncMock(return_value=mock_html)
    mock_context = AsyncMock()
    mock_context.__aenter__.return_value = mock_response
    session = MagicMock()
    session.get.return_value = mock_context
    return session


@pytest.fixture
def mock_xls() -> list[pd.DataFrame]:
    return generate_mock_xls_with_dates()


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
    files = [f"fake_{i}.xls" for i in range(1, FILES_COUNT + 1)]
    return SpimexParser(files=files)


@pytest.fixture
def loader(parser: SpimexParser) -> SpimexLoader:
    parser.parse()
    return SpimexLoader(sessionmaker=async_session_maker, df=parser.parsed_df, chunk_size=2)


@pytest.mark.asyncio
async def test_extract_links(collector, mock_session):
    links = await collector._extract_links(mock_session, "https://spimex.com/page1")
    assert all(link.startswith("https://spimex.com") for link in links)
    assert all(link.endswith(".xls") for link in links)
    assert len(links) > 0


@pytest.mark.asyncio
async def test_collect_links(monkeypatch, collector, queue):
    mock_data = [
        ["https://spimex.com/a.xls"],
        ["https://spimex.com/b.xls"],
        [],
    ]
    monkeypatch.setattr(collector, "_extract_links", AsyncMock(side_effect=mock_data))
    await collector.collect_links(workers=2)
    results = [queue.get_nowait() for _ in range(queue.qsize())]
    assert len([r for r in results if r]) == 2
    assert collector._extract_links.await_count == 3


def test_df_parsing(parser):
    parser.parse()
    parsed_df = parser.parsed_df
    assert not parsed_df.empty
    expected_columns = [
        "exchange_product_id",
        "exchange_product_name",
        "date",
        "count",
        "volume",
        "total",
        "delivery_basis_id",
        "delivery_basis_name",
        "delivery_type_id",
    ]
    for col in expected_columns:
        assert col in parsed_df.columns
    assert all(d.year == 2025 for d in parsed_df["date"])
    assert all(parsed_df["count"] > 0)


@pytest.mark.asyncio
async def test_load_data_to_db(loader):
    mock_session = AsyncMock()
    mock_session.__aenter__.return_value = mock_session
    mock_session.commit = AsyncMock()
    mock_session.rollback = AsyncMock()
    mock_session.merge = AsyncMock()
    mock_session.add_all = MagicMock()

    mock_sessionmaker = MagicMock(return_value=mock_session)
    loader.sessionmaker = mock_sessionmaker

    await loader.load()

    assert mock_sessionmaker.call_count > 0

    mock_session.commit.assert_called()
    assert mock_session.commit.await_count == mock_sessionmaker.call_count

    if loader.update_on_conflict:
        mock_session.merge.assert_called()
        mock_session.add_all.assert_not_called()
    else:
        mock_session.add_all.assert_called()
        mock_session.merge.assert_not_called()
