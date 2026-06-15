import logging
import os

from fail_detection import PORT, Node


def main():
    node_id = os.environ["NODE_ID"]
    peers_env = os.environ.get("PEERS", "")
    container_name = os.environ.get("CONTAINER_NAME", node_id)
    peer_containers_env = os.environ.get("PEER_CONTAINERS", "")

    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s [{node_id}] %(levelname)s %(message)s",
    )

    # Comma-separated hostnames, excluding self.
    peer_hosts = [
        p.strip() for p in peers_env.split(",") if p.strip() and p.strip() != node_id
    ]
    peers = [(host, host, PORT) for host in peer_hosts]

    # "node1:hb_node1,node2:hb_node2" -> {node1: hb_node1, ...}
    peer_containers: dict[str, str] = {}
    for entry in peer_containers_env.split(","):
        entry = entry.strip()
        if ":" in entry:
            nid, cname = entry.split(":", 1)
            peer_containers[nid.strip()] = cname.strip()

    logging.info("Node %s starting with peers: %s", node_id, peers)

    node = Node(
        node_id=node_id,
        peers=peers,
        container_name=container_name,
        peer_containers=peer_containers,
    )
    try:
        node.start()  # blocks in the monitor loop
    except KeyboardInterrupt:
        logging.info("Stopping node %s", node_id)
        node.stop()


if __name__ == "__main__":
    main()
