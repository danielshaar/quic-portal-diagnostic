"""
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt
uv run python quic_ping_pong.py [--small-payloads]
"""
import argparse
import asyncio
import modal
import os

from quic_portal import Portal, QuicTransportOptions

app = modal.App("pi-repro", image=modal.Image.debian_slim().pip_install("quic-portal==0.1.10"))


@app.function(timeout=20 * 60 * 60, region="us-east-1")
async def run_server(coord_dict: modal.Dict, small_payloads: bool):
    print(f"Starting server {os.getenv('MODAL_TASK_ID')}")
    transport_options = QuicTransportOptions(
        max_idle_timeout_secs=20,
        congestion_controller_type="cubic",
        initial_window=1024 * 1024,  # 1MiB
        keep_alive_interval_secs=1,
    )
    print(f"{transport_options=}")
    portal = Portal.create_server(dict=coord_dict, local_port=5555, transport_options=transport_options)
    print("Connected! Waiting for messages")

    message_count = 0
    while True:
        try:
            data = portal.recv()
        except Exception as e:
            print(f"Error receiving message: {e}")
            portal.close()
            return

        print(f"Received message {message_count}")
        await asyncio.sleep(0.05)  # Delay to simulate processing time.
        try:
            portal.send(b"b" * (1 if small_payloads else 60_000))
        except Exception as e:
            print(f"Error sending response: {e}")
            portal.close()
            return
        
        print(f"Sent response {message_count}")
        message_count += 1


async def run_client(coord_dict: modal.Dict, small_payloads: bool):
    print("Starting client")
    transport_options = QuicTransportOptions(
        max_idle_timeout_secs=20,
        congestion_controller_type="cubic",
        initial_window=1024 * 1024,  # 1MiB
        keep_alive_interval_secs=1,
    )
    print(f"{transport_options=}")
    portal = Portal.create_client(dict=coord_dict, local_port=5556, transport_options=transport_options)
    print("Connected! Sending messages")

    message_count = 0
    while True:
        print(f"Sending message {message_count}")
        try:
            portal.send(b"a" * (1 if small_payloads else 500_000))
        except Exception as e:
            print(f"Error sending message: {e}")
            portal.close()
            return

        try:
            _ = portal.recv()
        except Exception as e:
            print(f"Error receiving response: {e}")
            portal.close()
            return
        
        print(f"Received response {message_count}")
        message_count += 1


async def run_portal(small_payloads: bool = False):
    print("Starting portal run")
    
    with modal.Dict.ephemeral() as coord_dict:
        print("Spawning server")
        run_server.spawn(coord_dict, small_payloads)
        await asyncio.sleep(2)  # Give server time to start.

        print("Starting client")
        await run_client(coord_dict, small_payloads)


async def main(small_payloads: bool):
    while True:
        try:
            await run_portal(small_payloads)
        except Exception as e:
            print(f"Run stopped due to: {e}")
            continue

        print("Run completed")


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
