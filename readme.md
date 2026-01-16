export PYTHONPATH=$PYTHONPATH:.
celery -A worker_init worker -c 10 --loglevel=warning

export PYTHONPATH=$PYTHONPATH:.
python test_suite.py

redis-cli monitor

redis-cli flushall

~~export PYTHONPATH=$PYTHONPATH:.
python inspector.py --rate 0.2~~