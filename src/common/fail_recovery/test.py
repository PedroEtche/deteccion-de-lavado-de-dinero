import logging
import os

from fail_detection import Node


def main():
    node_id = os.environ["NODE_ID"]
    n_nodes = int(os.environ["N_NODES"])

    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s [{node_id}] %(levelname)s %(message)s",
    )

    peers = [
        (str(i), f"node{i}") for i in range(1, n_nodes + 1) if str(i) != node_id
    ]
    peer_containers = {
        str(i): f"node{i}" for i in range(1, n_nodes + 1) if str(i) != node_id
    }

    logging.info("Node %s starting with peers: %s", node_id, peers)

    node = Node(
        node_id=node_id,
        peers=peers,
        peer_containers=peer_containers,
    )
    try:
        node.start()
    except KeyboardInterrupt:
        logging.info("Stopping node %s", node_id)
        node.stop()


if __name__ == "__main__":
    main()
