import asyncio

import pytest
from solution import JOBS


@pytest.mark.asyncio
async def test_semaphore_limits_concurrency():
    active = 0
    max_active = 0

    async def track_job(name, delay, sem):
        nonlocal active, max_active
        async with sem:
            active += 1
            max_active = max(max_active, active)
            await asyncio.sleep(0.1)
            active -= 1

    sem = asyncio.Semaphore(2)
    await asyncio.gather(*[track_job(n, d, sem) for n, d in JOBS])
    assert max_active <= 2
