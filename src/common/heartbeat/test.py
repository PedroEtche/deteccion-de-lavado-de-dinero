import logging
import os

from heartbeat import PORT, Heartbeat


def main():
    node_id = os.environ["NODE_ID"]
    peers = [p.strip() for p in os.environ.get("PEERS", "").split(",") if p.strip()]
    port = int(os.environ.get("HEARTBEAT_PORT", PORT))
    interval = float(os.environ.get("HEARTBEAT_INTERVAL", 1.0))

    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s [{node_id}] %(levelname)s %(message)s",
    )

    # Send to every peer except ourselves.
    targets = [(peer, port) for peer in peers if peer != node_id]
    logging.info("Node %s starting with targets: %s", node_id, targets)

    heartbeat = Heartbeat(sender_id=node_id, targets=targets, interval=interval)
    try:
        heartbeat.start()  # blocks in the receive loop
    except KeyboardInterrupt:
        logging.info("Stopping node %s", node_id)
        heartbeat.stop()


if __name__ == "__main__":
    main()
