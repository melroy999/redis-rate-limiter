from config import init_infrastructure, app

# 1. Load the infrastructure (Registry + Redis)
init_infrastructure()

# 2. Register the tasks
app.conf.imports = [
    'src.rate_limiter.tasks.dispatcher',
    'src.rate_limiter.tasks.worker',
]
