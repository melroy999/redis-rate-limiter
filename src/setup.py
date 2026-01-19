from config import factory

# Create a test limiter.
test_limiter = factory.create_limiter(
    limiter_id="test_api",
    limit=50,
    window=10,
    max_concurrency=10,
    override=True
)