"""
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt
uv run python run_diagnostic.py
"""
import time

from subprocess import Popen


if __name__ == "__main__":
    try:
        network_check_process = Popen(["uv", "run", "python", "network_check.py"])
        quic_portal_check_process = Popen(["uv", "run", "python", "quic_portal_check.py"])

        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        network_check_process.terminate()
        quic_portal_check_process.terminate()
