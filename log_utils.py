import aiohttp
import asyncio

from datetime import datetime, timezone

LOG_URL = "https://modal-labs-daniel-dev--pi-diagnostics-recorder-record-di-fc1507.modal.run"


def record_log(log_queue, message):
    log_queue.append({
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "message": message
    })


async def send_logs_background(log_queue):
    async with aiohttp.ClientSession() as session:
        while True:
            if log_queue:
                log = log_queue.popleft()
                try:
                    async with session.post(LOG_URL, json=log) as response:
                        if response.status != 200:
                            raise Exception("Failed to send log")
                except Exception:
                    log_queue.appendleft(log)
                    await asyncio.sleep(5)
            else:
                await asyncio.sleep(1)
