## Установка
```bash
git clone https://github.com/etchedheadplate/spimex-scraper.git
cd spimex-scraper
pip install -r requirements.txt
```

## .env
```
DB_NAME = <name>
DB_HOST = <host>
DB_PORT = <port>
DB_USER = <username>
DB_PASS = <password>
```

## Запуск обновления базы данных
```bash
python3 update_db.py
```

## Запуск FastAPI
```bash
uvicorn main:app --host 127.0.0.1 --port 8000 --reload
```
