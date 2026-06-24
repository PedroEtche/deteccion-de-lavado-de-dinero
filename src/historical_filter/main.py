import logging
import os
from dataclasses import dataclass
from typing import Any, Dict

from src.common.communication.internal import (
    Q3ResultRow,
    build_batch_message,
    build_eof_message,
    serialize,
)
from src.common.middleware import MessageMiddlewareExchangeRabbitMQ
from src.common.utils import load_yaml_config
from src.common.state_manager import WorkerStateManager
from src.common.worker import StreamWorker, run_worker

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
    # Lo lee BaseWorker.__init__ (self.strategy = config.strategy); este worker
    # no usa strategy, asi que queda en None.
    strategy: Any = None


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


class HistoricalAverageFilter(StreamWorker):
    """
    Worker que junta DOS streams por payment_format y emite el resultado de Q3:

      - averages: promedio de monto por payment_format del rango [9/1, 9/5].
      - candidates: transacciones USD del rango [9/6, 9/15].

    Emite las candidatas cuyo monto sea menor a (promedio / threshold_divisor)
    para su mismo payment_format. No se puede filtrar hasta tener TODOS los
    promedios, asi que se bufferean ambos streams y se emite en el flush
    (cuando llegaron todos los EOFs).

    Hereda de StreamWorker: la infra de consumo/EOF/identidad la pone la base.
    Lo propio de este stage es distinguir de que stream viene cada batch (via
    msg_type, en handle_data) y emitir recien en on_flush; ademas bindea una
    routing key extra (data_broadcast) via input_routing_keys.
    """

    def __init__(self, config: HistoricalFilterConfig):
        super().__init__(config)
        self.output_mw = None

        # Umbrales por cliente (chico): thresholds_by_client[client][fmt] =
        # promedio / divisor. Vive en memoria pero se espeja a un snapshot en
        # disco para poder recuperarlo tras una caida.
        self.thresholds_by_client: Dict[str, Dict[str, float]] = {}

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

    def input_routing_keys(self) -> list:
        # Tres routing keys (la base por defecto solo bindea las dos primeras
        # variantes; aca agregamos data_broadcast):
        #   - worker_{id}: candidatas [9/6-9/15] que llegan round-robin del router.
        #   - data_broadcast: promedios que llegan por fanout del join.
        #   - eof_broadcast: EOFs de ambos upstreams (router + join).
        return [
            f"worker_{self.config.worker_id}",
            "data_broadcast",
            "eof_broadcast",
        ]

    def setup_outputs(self) -> None:
        self.output_mw = MessageMiddlewareExchangeRabbitMQ(
            self.config.mom_host, self.config.output_exchange
        )

    def close_outputs(self) -> None:
        if self.output_mw:
            self.output_mw.close()

    def handle_data(self, client_id: str, msg_type: str, batch: list, msg_id: int, sender: str) -> None:
        self._accumulate(client_id, msg_type, batch)

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

    def on_flush(self, client_id: str) -> None:
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

def main() -> int:
    config = init_config()
    logging.basicConfig(level=getattr(logging, config.log_level.upper(), logging.INFO))
    log_config(config)
    logging.getLogger("pika").setLevel(logging.WARNING)

    return run_worker(HistoricalAverageFilter(config))


if __name__ == "__main__":
    raise SystemExit(main())