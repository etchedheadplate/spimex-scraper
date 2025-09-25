## Установка
```bash
git clone https://github.com/etchedheadplate/spimex-scraper.git
cd spimex-scraper
python -m venv .venv
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

## Запуск обновления БД
```bash
python -m src.scripts.update_db
```

## Запуск Celery
```bash
celery -A src.worker.app.celery_app worker --loglevel=info
```
```bash
celery -A src.worker.app.celery_app beat --loglevel=info
```

## Запуск FastAPI
```bash
uvicorn main:app --host 127.0.0.1 --port 8000 --reload
```
