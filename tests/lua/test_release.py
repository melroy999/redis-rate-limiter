"""Tests for the ``release.lua`` Lua script.

Calls ``redis.eval()`` directly with controlled Redis state to verify
that the combined ZREM + DEL + PUBLISH cleanup behaves identically to
the legacy three-call Python path.

Fixture dependencies:
    - ``redis_client``: from ``tests/conftest.py``.
    - ``concurrency_key``, ``limiter_id``: from ``tests/conftest.py``
      and ``tests/lua/conftest.py``.
"""

import threading

import pytest

from tests.lua.conftest import RELEASE_SOURCE


def _eval_release(
    redis_client, concurrency_key, inflight_key, drain_channel, task_id, worker_id
):
    """Invoke ``release.lua`` via ``eval()`` with the given parameters."""
    return redis_client.eval(
        RELEASE_SOURCE,
        3,
        concurrency_key,
        inflight_key,
        drain_channel,
        task_id,
        worker_id,
    )


@pytest.fixture
def inflight_key(limiter_id: str) -> str:
    """Provide a per-test inflight key under the limiter namespace."""
    return f"rl:{limiter_id}:inflight:task-1"


@pytest.fixture
def drain_channel(limiter_id: str) -> str:
    """Provide a per-test drain signal channel under the limiter namespace."""
    return f"rl:{limiter_id}:drain_signal"


WORKER_ID = "worker-test-1"


@pytest.mark.behavior
class TestRelease:
    """Tests for the ``release.lua`` combined cleanup script."""

    @staticmethod
    def test_returns_one_one_when_both_present(
        redis_client, concurrency_key, inflight_key, drain_channel
    ):
        """Verify that release returns ``[1, 1]`` when both keys exist."""
        # Arrange
        redis_client.zadd(concurrency_key, {"task-1": 1000})
        redis_client.set(inflight_key, "1")

        # Act
        result = _eval_release(
            redis_client,
            concurrency_key,
            inflight_key,
            drain_channel,
            "task-1",
            WORKER_ID,
        )

        # Assert
        assert result == [1, 1], (
            "release should return [1, 1] when both concurrency and"
            " inflight entries exist"
        )
        assert redis_client.zscore(concurrency_key, "task-1") is None, (
            "task should be removed from the concurrency set"
        )
        assert redis_client.exists(inflight_key) == 0, (
            "inflight key should be deleted"
        )

    @staticmethod
    def test_returns_one_zero_when_only_concurrency_present(
        redis_client, concurrency_key, inflight_key, drain_channel
    ):
        """Verify that release returns ``[1, 0]`` when only the
        concurrency entry exists."""
        # Arrange
        redis_client.zadd(concurrency_key, {"task-1": 1000})

        # Act
        result = _eval_release(
            redis_client,
            concurrency_key,
            inflight_key,
            drain_channel,
            "task-1",
            WORKER_ID,
        )

        # Assert
        assert result == [1, 0], (
            "release should return [1, 0] when only the concurrency"
            " entry exists"
        )

    @staticmethod
    def test_returns_zero_one_when_only_inflight_present(
        redis_client, concurrency_key, inflight_key, drain_channel
    ):
        """Verify that release returns ``[0, 1]`` when only the
        inflight key exists."""
        # Arrange
        redis_client.set(inflight_key, "1")

        # Act
        result = _eval_release(
            redis_client,
            concurrency_key,
            inflight_key,
            drain_channel,
            "task-1",
            WORKER_ID,
        )

        # Assert
        assert result == [0, 1], (
            "release should return [0, 1] when only the inflight key exists"
        )

    @staticmethod
    def test_returns_zero_zero_when_neither_present(
        redis_client, concurrency_key, inflight_key, drain_channel
    ):
        """Verify that release returns ``[0, 0]`` when neither key exists."""
        # Act
        result = _eval_release(
            redis_client,
            concurrency_key,
            inflight_key,
            drain_channel,
            "task-1",
            WORKER_ID,
        )

        # Assert
        assert result == [0, 0], (
            "release should return [0, 0] when neither entry exists"
        )

    @staticmethod
    def test_empty_task_id_skips_zrem_and_del(
        redis_client, concurrency_key, inflight_key, drain_channel
    ):
        """Verify that an empty ``task_id`` performs no ZREM/DEL writes."""
        # Arrange
        redis_client.zadd(concurrency_key, {"task-1": 1000})
        redis_client.set(inflight_key, "1")

        # Act
        result = _eval_release(
            redis_client,
            concurrency_key,
            inflight_key,
            drain_channel,
            "",
            WORKER_ID,
        )

        # Assert
        assert result == [0, 0], (
            "release with empty task_id should return [0, 0]"
        )
        assert redis_client.zscore(concurrency_key, "task-1") == 1000, (
            "concurrency entry should be untouched when task_id is empty"
        )
        assert redis_client.exists(inflight_key) == 1, (
            "inflight key should be untouched when task_id is empty"
        )

    @staticmethod
    def test_only_named_task_removed_from_concurrency(
        redis_client, concurrency_key, inflight_key, drain_channel
    ):
        """Verify that ZREM only removes the named task, not its peers."""
        # Arrange
        redis_client.zadd(
            concurrency_key, {"task-1": 1000, "task-2": 2000, "task-3": 3000}
        )

        # Act
        _eval_release(
            redis_client,
            concurrency_key,
            inflight_key,
            drain_channel,
            "task-1",
            WORKER_ID,
        )

        # Assert
        assert redis_client.zscore(concurrency_key, "task-1") is None, (
            "named task should be removed from the concurrency set"
        )
        assert redis_client.zscore(concurrency_key, "task-2") == 2000, (
            "unrelated task-2 should remain in the concurrency set"
        )
        assert redis_client.zscore(concurrency_key, "task-3") == 3000, (
            "unrelated task-3 should remain in the concurrency set"
        )

    @staticmethod
    def test_publishes_worker_id_to_drain_channel(
        redis_client, concurrency_key, inflight_key, drain_channel
    ):
        """Verify that release publishes the worker id to the drain channel."""
        # Arrange
        # Subscribe in a background thread so we can capture the message
        # synchronously after the EVALSHA call.
        pubsub = redis_client.pubsub()
        pubsub.subscribe(drain_channel)
        # Drain the subscribe confirmation message.
        pubsub.get_message(timeout=1.0)

        received: list[dict] = []

        def _listen():
            msg = pubsub.get_message(timeout=2.0)
            if msg is not None:
                received.append(msg)

        listener = threading.Thread(target=_listen)
        listener.start()

        try:
            # Act
            _eval_release(
                redis_client,
                concurrency_key,
                inflight_key,
                drain_channel,
                "task-1",
                WORKER_ID,
            )
            listener.join(timeout=3.0)

            # Assert
            assert len(received) == 1, (
                "exactly one drain signal message should be published"
            )
            assert received[0]["data"] == WORKER_ID, (
                "published payload must equal the worker id passed in ARGV[2]"
            )
        finally:
            pubsub.unsubscribe(drain_channel)
            pubsub.close()

    @staticmethod
    def test_publishes_even_with_empty_task_id(
        redis_client, concurrency_key, inflight_key, drain_channel
    ):
        """Verify that the drain signal fires even when no cleanup occurred."""
        # Arrange
        pubsub = redis_client.pubsub()
        pubsub.subscribe(drain_channel)
        pubsub.get_message(timeout=1.0)

        received: list[dict] = []

        def _listen():
            msg = pubsub.get_message(timeout=2.0)
            if msg is not None:
                received.append(msg)

        listener = threading.Thread(target=_listen)
        listener.start()

        try:
            # Act
            _eval_release(
                redis_client,
                concurrency_key,
                inflight_key,
                drain_channel,
                "",
                WORKER_ID,
            )
            listener.join(timeout=3.0)

            # Assert
            assert len(received) == 1, (
                "drain signal must still be published when task_id is empty"
            )
        finally:
            pubsub.unsubscribe(drain_channel)
            pubsub.close()
