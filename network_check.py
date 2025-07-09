import asyncio
import time

from collections import deque
from log_utils import record_log, send_logs_background

CHECK_INTERVAL_SECS = 0.1  # 100ms


async def _probe_google():
    _, writer = await asyncio.open_connection("google.com", 80)
    writer.close()
    await writer.wait_closed()


async def check_network_loop():
    log_queue = deque()
    log_sender_task = asyncio.create_task(send_logs_background(log_queue))
    
    start_failure_time = None
    num_checks = 0
    check_durations = []
    while True:
        start_check_time = time.perf_counter()
        try:
            await asyncio.wait_for(_probe_google(), 5 * CHECK_INTERVAL_SECS)
            if start_failure_time:
                record_log(log_queue, f"[NETWORK] Connectivity resumed after {time.perf_counter() - start_failure_time:.2f}s")
                start_failure_time = None
        except asyncio.TimeoutError:
            record_log(log_queue, "[NETWORK] Connectivity check timed out")
            if not start_failure_time:
                start_failure_time = time.perf_counter()
        except Exception as e:
            record_log(log_queue, f"[NETWORK] Connectivity check failed: {e}")
            if not start_failure_time:
                start_failure_time = time.perf_counter()

        num_checks += 1
        check_duration = time.perf_counter() - start_check_time
        check_durations.append(check_duration)
        if num_checks == 50:
            record_log(log_queue, f"[NETWORK] Completed another 50 connectivity checks {check_durations}")
            num_checks = 0
            check_durations = []

        if check_duration < CHECK_INTERVAL_SECS:
            await asyncio.sleep(CHECK_INTERVAL_SECS - check_duration)


async def main():
    await check_network_loop()


if __name__ == "__main__":
    asyncio.run(main())
