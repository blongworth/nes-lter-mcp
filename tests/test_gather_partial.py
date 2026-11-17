import asyncio

from main import _gather_partial


def test_gather_partial_skips_failures_and_keeps_successes():
    async def work(item):
        if item == "bad":
            raise RuntimeError("boom")
        return item.upper()

    successes, errors = asyncio.run(_gather_partial(["good", "bad", "also-good"], work, lambda item: item))

    assert successes == [("good", "GOOD"), ("also-good", "ALSO-GOOD")]
    assert errors == ["bad: boom"]


def test_gather_partial_all_succeed_has_no_errors():
    async def work(item):
        return item + 1

    successes, errors = asyncio.run(_gather_partial([1, 2, 3], work, lambda item: item))

    assert successes == [(1, 2), (2, 3), (3, 4)]
    assert errors == []
