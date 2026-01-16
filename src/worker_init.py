# 1. Load the infrastructure (Registry + Redis)
from src.config import init_infrastructure, app

init_infrastructure()

# 2. Register the tasks
app.conf.imports = [
    'src.rate_limiter.tasks.dispatcher',
    'src.rate_limiter.tasks.worker',
]
