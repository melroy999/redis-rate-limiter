# 1. Load the infrastructure (Registry + Redis)
from config import celery_app, factory

# 2. Register the tasks
celery_app.conf.imports = [
    "celery_rate_limiter.tasks.dispatcher",
    "celery_rate_limiter.tasks.worker",
]

# Syncs the limiter settings with the worker
limiter = factory.registry.get("test_api")
limiter.drain()
