![Coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/melroy999/f3caa8f0af98bf11563b5b2031c1ef3e/raw/celery-rate-limiter-coverage.json)

export PYTHONPATH=$PYTHONPATH:examples:src
celery -A worker worker -c 10 --loglevel=warning

export PYTHONPATH=$PYTHONPATH:examples:src
python examples/simulation.py

redis-cli monitor

redis-cli flushall

export PYTHONPATH=$PYTHONPATH:examples:src
python examples/inspector.py --rate 0.2


# Run formatting
poetry run ruff check --select I --fix .
poetry run ruff format .
poetry run ruff format . --check
poetry run ruff check .

# Run type checks
poetry run mypy src
