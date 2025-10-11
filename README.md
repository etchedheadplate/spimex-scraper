## Установка
```bash
git clone -b develop https://github.com/etchedheadplate/spimex-scraper.git
cd spimex-scraper
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## .env
```
DB_NAME=your_db_name_here
DB_HOST=localhost
DB_PORT=5432
DB_USER=your_username_here
DB_PASS=your_password_here
```

## Обновление БД
```bash
python -m src.scripts.update_db
```

## Celery
```bash
celery -A src.worker.app.celery_app worker --beat --loglevel=info
```

## FastAPI
```bash
uvicorn main:app --host 127.0.0.1 --port 8000 --reload
```
