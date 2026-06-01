.PHONY: install setup ingest stream eod backfill-eod schedule-eod unschedule-eod all demo test dashboard bulk-ingest clean

install:
	pip install -r requirements.txt

setup:
	python setup.py

ingest:
	python ingest.py

stream:
	python stream.py

eod:
	python eod.py

backfill-eod:
	python eod.py --backfill

schedule-eod:
	python install_task.py

unschedule-eod:
	python install_task.py --suspend

all: setup ingest eod

demo: all
	streamlit run dashboard.py

test:
	pytest tests/ -v

dashboard:
	streamlit run dashboard.py

bulk-ingest:
	@if [ -z "$(FILE)" ]; then echo "usage: make bulk-ingest FILE=path/to/file.csv"; exit 1; fi
	python ingest_file.py $(FILE)

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type d -name .pytest_cache -exec rm -rf {} +
