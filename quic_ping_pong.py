"""
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt
uv run python quic_ping_pong.py [--small-payloads]
"""
import argparse
import asyncio
import logging
import modal
import os

from quic_portal import Portal, QuicTransportOptions

logger = logging.getLogger(__name__)
logging.basicConfig(format="[%(asctime)s] %(levelname)s: %(message)s", level=logging.INFO)

app = modal.App("pi-repro", image=modal.Image.debian_slim().pip_install("quic-portal==0.1.10"))


@app.function(timeout=30 * 60, region="us-sanjose-1")
async def run_server(coord_dict: modal.Dict, small_payloads: bool):
    logger.info(f"Starting server {os.getenv('MODAL_TASK_ID')}")
    transport_options = QuicTransportOptions(
        max_idle_timeout_secs=20,
        congestion_controller_type="cubic",
        initial_window=1024 * 1024,  # 1MiB
        keep_alive_interval_secs=1,
    )
    logger.info(f"{transport_options=}")
    portal = Portal.create_server(dict=coord_dict, local_port=5555, transport_options=transport_options)
    logger.info("Connected! Waiting for messages")

    message_count = 0
    while True:
        try:
            data = portal.recv()
        except Exception as e:
            logger.error(f"Error receiving message: {e}")
            portal.close()
            return

        logger.info(f"Received message {message_count}")
        await asyncio.sleep(0.05)  # Delay to simulate processing time.
        try:
            portal.send(b"b" * (1 if small_payloads else 60_000))
        except Exception as e:
            logger.error(f"Error sending response: {e}")
            portal.close()
            return
        
        logger.info(f"Sent response {message_count}")
        message_count += 1


async def run_client(coord_dict: modal.Dict, small_payloads: bool):
    logger.info("Starting client")
    transport_options = QuicTransportOptions(
        max_idle_timeout_secs=20,
        congestion_controller_type="cubic",
        initial_window=1024 * 1024,  # 1MiB
        keep_alive_interval_secs=1,
    )
    logger.info(f"{transport_options=}")
    portal = Portal.create_client(dict=coord_dict, local_port=5556, transport_options=transport_options)
    logger.info("Connected! Sending messages")

    message_count = 0
    while True:
        logger.info(f"Sending message {message_count}")
        try:
            portal.send(b"a" * (1 if small_payloads else 500_000))
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            portal.close()
            return

        try:
            _ = portal.recv()
        except Exception as e:
            logger.error(f"Error receiving response: {e}")
            portal.close()
            return
        
        logger.info(f"Received response {message_count}")
        message_count += 1


async def run_portal(small_payloads: bool = False):
    logger.info("Starting portal run")
    
    with modal.Dict.ephemeral() as coord_dict:
        logger.info("Spawning server")
        run_server.spawn(coord_dict, small_payloads)
        await asyncio.sleep(2)  # Give server time to start.

        logger.info("Starting client")
        await run_client(coord_dict, small_payloads)


async def main(small_payloads: bool):
    while True:
        try:
            await run_portal(small_payloads)
        except Exception as e:
            logger.error(f"Run stopped due to: {e}")
            continue

        logger.info("Run completed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="QUIC ping pong test")
    parser.add_argument(
        "--small-payloads",
        default=False,
        action="store_true",
        help="Use small payloads instead of large ones",
    )
    args = parser.parse_args()
    
    with app.run():
        asyncio.run(main(args.small_payloads))
