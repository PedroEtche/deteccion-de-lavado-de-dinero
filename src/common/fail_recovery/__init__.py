import os

from .fail_detection import Node


def node_from_env() -> Node:
    """Construye un Node de deteccion de fallas desde las env var

    node_id : id Bully de este worker. Lo provee el worker (su WORKER_ID, que
              ya es unico dentro del stage), no una env var aparte.
    FD_NODES   : miembros del grupo como "id:container" separados por coma,
                   incluido este mismo (se excluye solo).
    """
    node_id = os.environ["FD_NODE_ID"]
    members = [
        entry.split(":", 1) for entry in os.environ["FD_NODES"].split(",") if entry
    ]

    peers = [(nid, host) for nid, host in members if nid != node_id]
    peer_containers = {nid: host for nid, host in members if nid != node_id}

    return Node(node_id=node_id, peers=peers, peer_containers=peer_containers)
