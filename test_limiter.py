import redis

from src.rate_limiter.base import SlidingWindowRateLimiter

REDIS_URL = 'redis://localhost:6379/0'
r = redis.Redis.from_url(REDIS_URL, decode_responses=True)

# Mocking two different instances/services using the same Redis key
limiter_instance_a = SlidingWindowRateLimiter(r, "api_global", limit=3, window=10)
limiter_instance_b = SlidingWindowRateLimiter(r, "api_global", limit=3, window=10)

# Instance A takes 2 slots
print(limiter_instance_a.check()) # remaining: 2
print(limiter_instance_a.check()) # remaining: 1

# Instance B tries to take 2 slots but only 1 is left
print(limiter_instance_b.check()) # remaining: 0
print(limiter_instance_b.check()) # allowed: False