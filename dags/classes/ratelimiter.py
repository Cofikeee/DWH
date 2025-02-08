import asyncio
import time


class RateLimiter:
    def __init__(self, min_interval: float = 0.05):
        self.min_interval = min_interval  # Минимальный интервал между запросами
        self.last_request_time = 0  # Время последнего запроса
        self.lock = asyncio.Lock()  # Блокировка для синхронизации

    async def wait(self):
        async with self.lock:
            now = time.monotonic()
            elapsed = now - self.last_request_time

            if elapsed < self.min_interval:
                await asyncio.sleep(self.min_interval - elapsed)

            self.last_request_time = time.monotonic()
