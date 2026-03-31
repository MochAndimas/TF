PYTHON ?= python3

.PHONY: init-db test backend frontend sqlite-backup

init-db:
	$(PYTHON) init_db.py

test:
	$(PYTHON) -m pytest -q

backend:
	$(PYTHON) main.py

frontend:
	streamlit run streamlit_run.py --server.port 5504

sqlite-backup:
	$(PYTHON) scripts/backup_sqlite.py
