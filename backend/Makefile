DB = pandb

dev:
	fastapi dev main.py

req:
	python3 -m pip install -r requirements.txt

models:
	python3 -m pwiz -e postgresql $(DB) > pandb_models.py
