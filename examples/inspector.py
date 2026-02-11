import argparse
import os
import time

from config import configure_limiter

from celery_rate_limiter.backends.celery.limiter import CeleryRateLimiter


def run_inspector(refresh_rate: float) -> None:
    """Run the rate limiter inspector to monitor limiter status in real-time.

    Args:
        refresh_rate: The refresh rate in seconds for updating the display.
    """
    configure_limiter()

    # Get the shared limiter instance.
    # This connects to the same Redis and uses the same keys as your worker
    limiter = CeleryRateLimiter.get("test_api")

    try:
        while True:
            # Fetch data from Redis via the limiter object.
            status = limiter.get_status()

            os.system("clear" if os.name == "posix" else "cls")

            print(f"=== MONITORING: {limiter.id} ===")
            print(f"Time: {time.strftime('%H:%M:%S')}")

            # Concurrency Stats
            c = status["concurrency"]
            bar_width = 20
            filled = int((c["current"] / c["max"]) * bar_width) if c["max"] > 0 else 0
            bar = "█" * filled + "░" * (bar_width - filled)
            print(f"\nCONCURRENCY: [{bar}] {c['current']}/{c['max']}")

            # Buffer Stats
            b = status["buffer"]
            print(f"BUFFER:      {b['count']} tasks waiting")

            # Rate Limit Stats
            r = status["rate_limit"]

            # Format to 1 decimal place since it's an estimation
            used_str = f"{r['tokens_used']:.1f}"
            limit_str = f"{r['limit']}"

            # Visual Bar for Rate Limit
            pct = min(1.0, r["tokens_used"] / r["limit"])
            bar_len = 20
            filled = int(pct * bar_len)
            bar = "█" * filled + "░" * (bar_len - filled)

            print(f"RATE LIMIT:  [{bar}] {used_str}/{limit_str}")
            if r["tokens_used"] >= r["limit"]:
                print(
                    f"             STATUS: SATURATED (Window rotates in {r['reset_in_ms']}ms)"
                )
            else:
                print(
                    f"             STATUS: OK (Previous: {r['val_previous']}, Current: {r['val_current']})"
                )

            # Lock Status
            lock = "BUSY" if status["dispatcher"]["is_locked"] else "IDLE"
            print(f"DISPATCHER:  {lock}")

            print("\n" + "-" * 35)
            print("Press [Ctrl+C] to exit")

            time.sleep(refresh_rate)

    except KeyboardInterrupt:
        print("\nStopping inspector...")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--rate", type=float, default=0.5, help="Refresh rate (seconds)"
    )

    args = parser.parse_args()
    run_inspector(args.rate)
