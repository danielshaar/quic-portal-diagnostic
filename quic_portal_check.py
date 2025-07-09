import aiohttp
import asyncio
import logging
import modal
import os

from collections import deque
from log_utils import record_log, send_logs_background
from quic_portal import Portal, QuicTransportOptions

app = modal.App("pi-repro", image=modal.Image.debian_slim().pip_install("quic-portal==0.1.9").add_local_python_source("log_utils"))

TRANSPORT_OPTIONS_URL = "https://modal-labs-daniel-dev--pi-diagnostics-recorder-fetch-tra-998f21.modal.run"


async def _fetch_transport_options():
    async with aiohttp.ClientSession() as session:
        async with session.get(TRANSPORT_OPTIONS_URL) as response:
            if response.status != 200:
                raise Exception("Failed to fetch transport options")

            return await response.json()


@app.function(timeout=30 * 60, region="us-sanjose-1")
async def run_server(coord_dict: modal.Dict):
    logger = logging.getLogger(__name__)
    logging.basicConfig(format="[%(asctime)s] %(message)s", level=logging.INFO)

    logger.info(f"Starting server {os.getenv('MODAL_TASK_ID')}")
    transport_options = await _fetch_transport_options()
    logger.info(f"Transport options: {transport_options}")
    portal = Portal.create_server(dict=coord_dict, local_port=5555, transport_options=QuicTransportOptions(**transport_options))
    logger.info("Connected! Waiting for messages")

    while True:
        try:
            data = portal.recv()
        except Exception as e:
            logger.error(f"Error receiving message: {e}")
            return

        logger.info(f"Received message")
        await asyncio.sleep(0.05)  # Delay to simulate processing time.
        portal.send(b"b" * 60_000)
        logger.info(f"Sent response")


async def run_client(coord_dict: modal.Dict, log_queue):
    record_log(log_queue, "[QUIC] Starting client")
    transport_options = await _fetch_transport_options()
    record_log(log_queue, f"[QUIC] Transport options: {transport_options}")
    portal = Portal.create_client(dict=coord_dict, local_port=5556, transport_options=QuicTransportOptions(**transport_options))
    record_log(log_queue, "[QUIC] Connected! Sending messages")

    for _ in range(10_000):
        record_log(log_queue, "[QUIC] Sending message")
        portal.send(b"a" * 500_000)

        _ = portal.recv()
        record_log(log_queue, "[QUIC] Received response")

    portal.close()


async def run_portal(log_queue):
    record_log(log_queue, "[QUIC] Starting portal run")
    
    with modal.Dict.ephemeral() as coord_dict:
        record_log(log_queue, "[QUIC] Spawning server")
        run_server.spawn(coord_dict)
        await asyncio.sleep(2)  # Give server time to start.

        record_log(log_queue, "[QUIC] Starting client")
        await run_client(coord_dict, log_queue)


async def main():
    log_queue = deque()
    log_sender_task = asyncio.create_task(send_logs_background(log_queue))
    while True:
        try:
            await run_portal(log_queue)
        except Exception as e:
            record_log(log_queue, f"[QUIC] Run stopped due to: {e}")
            continue

        record_log(log_queue, "[QUIC] Run completed")


if __name__ == "__main__":
    with app.run():
        asyncio.run(main())
