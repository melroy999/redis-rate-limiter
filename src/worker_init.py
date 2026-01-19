# 1. Load the infrastructure (Registry + Redis)
from src.config import celery_app

# 2. Register the tasks
celery_app.conf.imports = [
    'src.rate_limiter.tasks.dispatcher',
    'src.rate_limiter.tasks.worker',
]
