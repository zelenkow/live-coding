import asyncio
import time

SEMAPHORE = 2

JOBS = [
    ("task-1", 2),
    ("task-2", 1),
    ("task-3", 3),
    ("task-4", 2),
    ("task-5", 1),
]


async def process_job(name: str, delay: int, sem: asyncio.Semaphore) -> None:
    async with sem:
        print(f"{name} started")
        await asyncio.sleep(delay)
        print(f"{name} finished")


async def main() -> None:
    start = time.perf_counter()

    sem = asyncio.Semaphore(SEMAPHORE)
    await asyncio.gather(*[process_job(name, delay, sem) for name, delay in JOBS])

    elapsed = time.perf_counter() - start
    print(f"All jobs completed at {elapsed:.2f}s")


if __name__ == "__main__":
    asyncio.run(main())
