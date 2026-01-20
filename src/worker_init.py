# 1. Load the infrastructure (Registry + Redis)
from src.config import celery_app

# 2. Register the tasks
celery_app.conf.imports = [
    'src.celery_rate_limiter.tasks.dispatcher',
    'src.celery_rate_limiter.tasks.worker',
]
