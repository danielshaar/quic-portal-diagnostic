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

logger = logging.getLogger("quic_ping_pong")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s"))
    logger.addHandler(handler)
    logger.propagate = False

app = modal.App("pi-repro", image=modal.Image.debian_slim().pip_install("quic-portal==0.1.11"))


@app.function(timeout=20 * 60 * 60, region="us-sanjose-1")
async def run_server(coord_dict: modal.Dict, small_payloads: bool, use_random_delay: bool, port: int):
    logger.info(f"Starting server {os.getenv('MODAL_TASK_ID')}")
    transport_options = QuicTransportOptions(
        max_idle_timeout_secs=20,
        congestion_controller_type="cubic",
        initial_window=1024 * 1024,  # 1MiB
        keep_alive_interval_secs=1,
    )
    portal = Portal.create_server(dict=coord_dict, local_port=port, transport_options=transport_options)
    logger.info("Connected! Waiting for messages")

    message_count = 0
    while True:
        try:
            data = portal.recv()
        except Exception as e:
            logger.error(f"Error receiving message {message_count}: {e}")
            portal.close()
            return

        if use_random_delay or message_count % 100 == 0:
            logger.info(f"Received message {message_count}")

        await asyncio.sleep(0.05)  # Delay to simulate processing time.
        try:
            portal.send(b"b" * (1 if small_payloads else 60_000))
        except Exception as e:
            logger.error(f"Error sending response {message_count}: {e}")
            portal.close()
            return
        
        if use_random_delay or message_count % 100 == 0:
            logger.info(f"Sent response {message_count}")

        message_count += 1


async def run_client(coord_dict: modal.Dict, small_payloads: bool, use_random_delay: bool, port: int):
    logger.info("Starting client")
    transport_options = QuicTransportOptions(
        max_idle_timeout_secs=20,
        congestion_controller_type="cubic",
        initial_window=1024 * 1024,  # 1MiB
        keep_alive_interval_secs=1,
    )
    portal = Portal.create_client(dict=coord_dict, local_port=port, transport_options=transport_options)
    logger.info("Connected! Sending messages")

    message_count = 0
    while True:
        if use_random_delay or message_count % 100 == 0:
            logger.info(f"Sending message {message_count}")

        try:
            portal.send(b"a" * (1 if small_payloads else 500_000))
        except Exception as e:
            logger.error(f"Error sending message {message_count}: {e}")
            portal.close()
            return

        try:
            _ = portal.recv()
        except Exception as e:
            logger.error(f"Error receiving response {message_count}: {e}")
            portal.close()
            return
        
        if use_random_delay or message_count % 100 == 0:
            logger.info(f"Received response {message_count}")

        message_count += 1
        delay = random.choice([0, 0.1, 0.25, 1, 10, 120])
        logger.info(f"Sleeping for {delay} seconds")
        await asyncio.sleep(delay)


async def run_portal(small_payloads: bool = False, use_random_delay: bool = False, port: int = 5555):
    logger.info("Starting portal run")
    
    with modal.Dict.ephemeral() as coord_dict:
        logger.info(f"Spawning server on {port=}")
        run_server.spawn(coord_dict, small_payloads, use_random_delay, port)
        await asyncio.sleep(2)  # Give server time to start.

        logger.info(f"Starting client on {port=}")
        await run_client(coord_dict, small_payloads, use_random_delay, port + 1)


async def main(small_payloads: bool, use_random_delay: bool, port: int):
    while True:
        try:
            await run_portal(small_payloads, use_random_delay, port)
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
    parser.add_argument(
        "--use-random-delay",
        default=False,
        action="store_true",
        help="Use random delay between messages",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=5555,
        help="Port to use for the QUIC server/client (default: 5555)",
    )
    args = parser.parse_args()
    
    with app.run():
        asyncio.run(main(args.small_payloads, args.use_random_delay, args.port))
