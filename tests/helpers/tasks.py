"""Test-only task functions used by async backend tests."""

import asyncio


def noop_task(**kwargs):
    """A no-op task that accepts any keyword arguments and returns immediately."""
    pass


def noop_task_2(**kwargs):
    """A second no-op task with a distinct function path for deduplication tests."""
    pass


async def slow_task(**kwargs):
    """An async task that sleeps for a long duration, used for cancellation tests."""
    await asyncio.sleep(3600)
