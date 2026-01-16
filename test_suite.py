from config import init_infrastructure
import time

# Syncs the limiter settings with the worker
_, limiter = init_infrastructure()


# Define a "Business Logic" function to be called
# In a real app, this would be in app/services.py
def mock_api_call(user_id: int, error: bool = False):
    print(f" >>> [WORKER] Starting API call for user {user_id}")
    time.sleep(2)  # Simulate a slow network request
    print(f" <<< [WORKER] Finished API call for user {user_id}")
    if error:
        raise Exception("API call failed")
    return True


if __name__ == "__main__":
    print("--- Starting Simulation ---")

    # Test Deduplication: Add the same user 5 times
    print("Pushing 50 duplicate tasks...")
    for _ in range(50):
        limiter.schedule_task("test_suite.mock_api_call", {"user_id": 1})

    # Test Burst: Add 5 different users
    print("Pushing 5 unique tasks...")
    for i in range(2, 50):
        limiter.schedule_task("test_suite.mock_api_call", {"user_id": i})

    limiter.schedule_task("test_suite.mock_api_call", {"user_id": 1000000, "error": True})

    print("--- Simulation Queued. Check Celery logs for staggered execution ---")