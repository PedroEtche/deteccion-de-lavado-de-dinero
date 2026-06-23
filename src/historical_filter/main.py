import logging
import threading
import os
import signal
from dataclasses import dataclass
from typing import Dict

from src.common import fail_recovery
from src.common.communication.internal import (
    Q3ResultRow,
    build_batch_message,
    build_eof_message,
    deserialize,
    serialize,
)
from src.common.eof import EofCoordinator
from src.common.middleware import MessageMiddlewareExchangeRabbitMQ
from src.common.middleware.middleware_rabbitmq import CONSUMER_HEARTBEAT
from src.common.utils import load_yaml_config
from src.common.state_manager import WorkerStateManager

CONFIG_PATH = "./config.yaml"

# msg_type de cada uno de los dos streams de entrada:
#   - los promedios por payment_format vienen del aggregator como "batch"
#     (build_batch_message), con items dict {payment_format, average_amount}.
#   - las transacciones candidatas del rango [9/6, 9/15] vienen como
#     "raw_transactions" (build_raw_transactions_message), items TransactionRow.
AVERAGES_MSG_TYPE = "batch"
CANDIDATES_MSG_TYPE = "raw_transactions"

#
RESULT_BATCH_SIZE = 5000


@dataclass
class HistoricalFilterConfig:
    mom_host: str
    input_exchange: str
    output_exchange: str
    threshold_divisor: float
    expected_eofs: int
    worker_id: int
    num_downstream_workers: int
    log_level: str
    stage_name: str


def init_config() -> HistoricalFilterConfig:
    data = load_yaml_config(CONFIG_PATH)
    return HistoricalFilterConfig(
        mom_host=data.get("mom_host", "rabbitmq"),
        input_exchange=data.get("input", ""),
        output_exchange=data.get("output", ""),
        threshold_divisor=float(data.get("threshold_divisor", 100)),
        expected_eofs=int(os.getenv("EOF_EXPECTED", "1")),
        worker_id=int(os.getenv("WORKER_ID", "1")),
        num_downstream_workers=int(os.getenv("NUM_DOWNSTREAM_WORKERS", "1")),
        log_level=os.environ.get("LOG_LEVEL", "INFO"),
        stage_name=os.environ.get("STAGE_NAME", "historical_filter"),
    )


def log_config(config: HistoricalFilterConfig) -> None:
    logging.info(
        "HistoricalFilter startup: mom_host=%s | input=%s | output=%s | "
        "threshold_divisor=%s | expected_eofs=%d | worker_id=%d",
        config.mom_host,
        config.input_exchange,
        config.output_exchange,
        config.threshold_divisor,
        config.expected_eofs,
        config.worker_id,
    )


class HistoricalAverageFilter:
    """
    Ad-hoc worker (no hereda de BaseWorker) que junta DOS streams por
    payment_format y emite el resultado de Q3:

      - averages: promedio de monto por payment_format del rango [9/1, 9/5].
      - candidates: transacciones USD del rango [9/6, 9/15].

    Emite las candidatas cuyo monto sea menor a (promedio / threshold_divisor)
    para su mismo payment_format. No se puede filtrar hasta tener TODOS los
    promedios, asi que se bufferean ambos streams y se emite en el flush
    (cuando llegaron todos los EOFs).

    No usa BaseWorker porque este stage necesita distinguir de que stream
    viene cada batch (via msg_type) Y emitir en el flush. Consideramos que era mejor
    no modificar las otras abstracciones y codear esta clase especifica.
    """

    def __init__(self, config: HistoricalFilterConfig):
        self.config = config
        self.input_mw = None
        self.output_mw = None
        self.sender_id = f"{config.stage_name}_{config.worker_id}"
        # Contador monotonico por sender (msg_id entero creciente desde 0).
        self.msg_counter = 0

        # Umbrales por cliente (chico): thresholds_by_client[client][fmt] =
        # promedio / divisor. Vive en memoria pero se espeja a un snapshot en
        # disco para poder recuperarlo tras una caida.
        self.thresholds_by_client: Dict[str, Dict[str, float]] = {}

        self.eof_state_manager = WorkerStateManager(
            base_dir="/app/state",
            stage_name=f"{self.config.stage_name}_eof",
            worker_id=self.config.worker_id,
        )
        # Candidatas [9/6-9/15]: se vuelcan al WAL en disco a medida que llegan
        # (no se acumulan en memoria) y se releen en streaming en el flush.
        self.candidates_state = WorkerStateManager(
            base_dir="/app/state",
            stage_name=f"{self.config.stage_name}_candidates",
            worker_id=self.config.worker_id,
        )
        # Umbrales: se snapshotean (no comparten manager con las candidatas
        # porque save_snapshot trunca el WAL).
        self.thresholds_state = WorkerStateManager(
            base_dir="/app/state",
            stage_name=f"{self.config.stage_name}_thresholds",
            worker_id=self.config.worker_id,
        )

        # Recuperacion al arranque: restaurar los umbrales de cada cliente desde
        # su snapshot. Las candidatas ya estan en el WAL (se leen en el flush) y
        # el conteo de EOFs lo recupera el EofCoordinator por su cuenta.
        for client_id in self.thresholds_state.get_all_client_ids():
            snapshot, _ = self.thresholds_state.recover_client(client_id)
            if snapshot:
                self.thresholds_by_client[client_id] = snapshot
                logging.info("Recovered thresholds for client %s", client_id)

    def _next_msg_id(self) -> int:
        msg_id = self.msg_counter
        self.msg_counter += 1
        return msg_id

    def start(self) -> None:
        self._start_fail_detection()

        self.output_mw = MessageMiddlewareExchangeRabbitMQ(
            self.config.mom_host, self.config.output_exchange
        )

        self.eof_coordinator = EofCoordinator(
            expected_eofs=self.config.expected_eofs,
            on_flush=self._on_flush,
            state_manager=self.eof_state_manager,
        )

        # Tres routing keys:
        #   - worker_{id}: candidatas [9/6-9/15] que llegan round-robin del router.
        #   - data_broadcast: promedios que llegan por fanout del join.
        #   - eof_broadcast: EOFs de ambos upstreams (router + join).
        routing_keys = [
            f"worker_{self.config.worker_id}",
            "data_broadcast",
            "eof_broadcast",
        ]
        self.input_mw = MessageMiddlewareExchangeRabbitMQ(
            host=self.config.mom_host,
            exchange_name=self.config.input_exchange,
            routing_keys=routing_keys,
            heartbeat=CONSUMER_HEARTBEAT,  # consumer: RabbitMQ detecta caidas y re-encola
        )

        logging.info("HistoricalFilter listening on %s", self.config.input_exchange)
        self.input_mw.start_consuming(self._on_message)

    def _start_fail_detection(self):
        """Start fail detection.
        node_id is the worker_id (unique within the stage) and the peers come from the environment variables.
        Node.start() blocks (runs its monitoring loop), so it runs in a daemon thread to
        avoid blocking message consumption.
        """
        self.fd_node = fail_recovery.node_from_env()
        threading.Thread(
            target=self.fd_node.start, daemon=True, name="fail-detection"
        ).start()
        logging.info("Fail detection daemon started")

    def _on_message(self, message, ack, nack) -> None:
        try:
            decoded = deserialize(message)
            msg_type = decoded.get("type")
            client_id = decoded.get("client")

            if msg_type == "eof":
                logging.info("EOF received for client %s", client_id)
                self.eof_coordinator.handle_eof(client_id)
            else:
                batch = decoded.get("payload", {}).get("batch", [])
                self._accumulate(client_id, msg_type, batch)
            ack()
        except Exception:
            logging.exception("Error processing message; nack")
            nack()

    def _accumulate(self, client_id: str, msg_type: str, batch: list) -> None:
        if msg_type == AVERAGES_MSG_TYPE:
            thresholds = self.thresholds_by_client.setdefault(client_id, {})
            for avg in batch:
                fmt = avg["payment_format"]
                thresholds[fmt] = avg["average_amount"] / self.config.threshold_divisor
            # Snapshot de los umbrales (antes del ack) para poder recuperarlos.
            self.thresholds_state.save_snapshot(client_id, thresholds)
            logging.info("Stored %d averages for client %s", len(batch), client_id)

        elif msg_type == CANDIDATES_MSG_TYPE:
            # Las candidatas van directo al WAL en disco (con fsync), no a memoria.
            self.candidates_state.append_batch(client_id, batch)
            logging.info(
                "Buffered %d candidate txs for client %s", len(batch), client_id
            )

        else:
            logging.warning("Unexpected msg_type %s for client %s", msg_type, client_id)

    def _on_flush(self, client_id: str) -> None:
        thresholds = self.thresholds_by_client.pop(client_id, {})

        candidate_count = 0
        emitted_count = 0
        result_batch = []
        # Las candidatas se releen en streaming desde el WAL en disco; vienen
        # como dicts (no TransactionRow), asi que se accede por clave.
        for wal_batch in self.candidates_state.iter_wal_batches(client_id):
            for tx in wal_batch:
                candidate_count += 1
                threshold = thresholds.get(tx.get("payment_format"))
                if threshold is None:
                    # No hay promedio para ese formato en [9/1, 9/5]; no se puede
                    # comparar, se descarta.
                    continue
                if (tx.get("amount_paid") or 0.0) < threshold:
                    # Proyeccion al resultado de Q3. El orden de columnas del CSV
                    # lo define el orden de campos de Q3ResultRow.
                    result_batch.append(
                        Q3ResultRow(
                            from_bank=tx.get("from_bank"),
                            from_account=tx.get("from_account"),
                            payment_format=tx.get("payment_format"),
                            amount_paid=tx.get("amount_paid"),
                        )
                    )
                    # Emitir por chunks: un unico mensaje con todo el resultado
                    # supera el max message size de RabbitMQ con datasets grandes.
                    if len(result_batch) >= RESULT_BATCH_SIZE:
                        self._send_result_batch(client_id, result_batch)
                        emitted_count += len(result_batch)
                        result_batch = []

        if result_batch:
            self._send_result_batch(client_id, result_batch)
            emitted_count += len(result_batch)

        logging.info(
            "Flush client %s: %d candidates -> %d below threshold",
            client_id,
            candidate_count,
            emitted_count,
        )

        eof_msg = serialize(
            build_eof_message(
                client=client_id, msg_id=self._next_msg_id(), sender=self.sender_id
            )
        )
        self.output_mw.send(eof_msg, routing_key="eof_broadcast")

        # Estado del cliente ya consumido: liberar candidatas (WAL) y umbrales
        # (snapshot) en disco. El conteo de EOFs lo limpia el EofCoordinator.
        self.candidates_state.delete_client(client_id)
        self.thresholds_state.delete_client(client_id)

    def _send_result_batch(self, client_id: str, result: list) -> None:
        # msg_type "batch": tipo generico ya usado entre workers (group,
        # aggregator). No dispara conversion a TransactionRow en el join, que lo
        # reenvuelve como q3_result (QueryResultStrategy 3).
        msg_id = self._next_msg_id()
        out_msg = serialize(
            build_batch_message(
                "batch",
                client=client_id,
                msg_id=msg_id,
                batch=result,
                sender=self.sender_id,
            )
        )
        # Round-robin determinista: worker destino = msg_id % N.
        target_worker = (msg_id % self.config.num_downstream_workers) + 1
        self.output_mw.send(out_msg, routing_key=f"worker_{target_worker}")

    def stop(self) -> None:
        if self.input_mw:
            self.input_mw.stop_consuming()
            self.input_mw.close()
        if self.output_mw:
            self.output_mw.close()


def main() -> int:
    config = init_config()
    logging.basicConfig(level=getattr(logging, config.log_level.upper(), logging.INFO))
    log_config(config)
    logging.getLogger("pika").setLevel(logging.WARNING)

    worker = HistoricalAverageFilter(config)

    def handle_sigterm(_signum, _frame):
        logging.info("Received SIGTERM signal")
        worker.stop()

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)

    worker.start()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
