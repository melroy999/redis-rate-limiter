from config import factory

# Create a test limiter.
test_limiter = factory.create_limiter(
    limiter_id="test_api", limit=5, window=1, max_concurrency=100, override=True
)
