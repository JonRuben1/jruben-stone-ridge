.PHONY: install setup ingest stream eod backfill-eod schedule-eod unschedule-eod all demo test dashboard bulk-ingest clean

VENV := .venv
PY := $(VENV)/bin/python

$(VENV)/bin/python:
	python3 -m venv $(VENV)
	$(VENV)/bin/python -m pip install --upgrade pip

install: $(VENV)/bin/python
	$(PY) -m pip install -r requirements.txt

setup:
	$(PY) src/setup.py

ingest:
	$(PY) src/ingest.py

stream:
	$(PY) src/stream.py

eod:
	$(PY) src/eod.py

backfill-eod:
	$(PY) src/eod.py --backfill

schedule-eod:
	$(PY) src/install_task.py

unschedule-eod:
	$(PY) src/install_task.py --suspend

all: setup ingest eod

demo: all
	$(PY) -m streamlit run src/dashboard.py

test:
	$(PY) -m pytest tests/ -v

dashboard:
	$(PY) -m streamlit run src/dashboard.py

bulk-ingest:
	@if [ -z "$(FILE)" ]; then echo "usage: make bulk-ingest FILE=path/to/file.csv"; exit 1; fi
	$(PY) src/ingest_file.py $(FILE)

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type d -name .pytest_cache -exec rm -rf {} +
