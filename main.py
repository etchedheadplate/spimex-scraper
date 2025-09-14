import os
from datetime import datetime

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.loader import SpimexLoader
from src.models import BaseModel
from src.parser import SpimexParser
from src.scraper import SpimexDownloader

load_dotenv()

DB_NAME = os.environ.get("DB_NAME")
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT")
DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASS")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
BaseModel.metadata.create_all(engine)

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

start = datetime(2023, 1, 1)
end = datetime.today()
downloader = SpimexDownloader(start, end)
# downloader.run()
parser = SpimexParser()
df = parser.parse("bulletins/oil_xls_20230109162000.xls")
loader = SpimexLoader(session)
loader.load(df)
