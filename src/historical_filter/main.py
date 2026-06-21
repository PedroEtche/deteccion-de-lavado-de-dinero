import logging
import os
import signal
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List

from src.common.communication.internal import (
    Q3ResultRow,
    build_batch_message,
    build_eof_message,
    deserialize,
    serialize,
)
from src.common.eof import EofCoordinator
from src.common.middleware import MessageMiddlewareExchangeRabbitMQ
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
        self.current_downstream_worker = 1

        # Estado por cliente.
        # thresholds_by_client[client][payment_format] = promedio / divisor
        self.thresholds_by_client: Dict[str, Dict[str, float]] = {}
        # TODO: candidates_by_client crece sin limite mientras esperamos los
        # promedios del otro stream. Con el dataset completo esto puede
        # quedarse sin memoria. Hay que persistir las candidatas a disco
        # (append a un archivo por cliente) a medida que llegan, y releerlas
        # en el flush. Por ahora se mantienen en memoria.
        self.candidates_by_client: Dict[str, List[Any]] = {}

        self.eof_state_manager = WorkerStateManager(
            base_dir="/app/state",
            stage_name=f"{self.config.stage_name}_eof",
            worker_id=self.config.worker_id,
        )

    def start(self) -> None:
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
        )

        logging.info("HistoricalFilter listening on %s", self.config.input_exchange)
        self.input_mw.start_consuming(self._on_message)

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
            logging.info("Stored %d averages for client %s", len(batch), client_id)

        elif msg_type == CANDIDATES_MSG_TYPE:
            # TODO: en vez de acumular en memoria, persistir a disco aca.
            self.candidates_by_client.setdefault(client_id, []).extend(batch)
            logging.info(
                "Buffered %d candidate txs for client %s", len(batch), client_id
            )

        else:
            logging.warning("Unexpected msg_type %s for client %s", msg_type, client_id)

    def _on_flush(self, client_id: str) -> None:
        thresholds = self.thresholds_by_client.pop(client_id, {})
        # TODO: cuando las candidatas esten en disco, releerlas aca en vez de
        # tomarlas de memoria.
        candidates = self.candidates_by_client.pop(client_id, [])

        result = []
        for tx in candidates:
            threshold = thresholds.get(tx.payment_format)
            if threshold is None:
                # No hay promedio para ese formato en [9/1, 9/5]; no se puede
                # comparar, se descarta.
                continue
            if (tx.amount_paid or 0.0) < threshold:
                # Proyeccion al resultado de Q3. El orden de columnas del CSV lo
                # define el orden de campos de Q3ResultRow.
                result.append(  # TODO cambiar a que sea un batch, la generacion de Q3ResultRow la hace el join
                    Q3ResultRow(
                        from_bank=tx.from_bank,
                        from_account=tx.from_account,
                        payment_format=tx.payment_format,
                        amount_paid=tx.amount_paid,
                    )
                )

        logging.info(
            "Flush client %s: %d candidates -> %d below threshold",
            client_id,
            len(candidates),
            len(result),
        )

        if result:
            # msg_type "batch": tipo generico ya usado entre workers (group,
            # aggregator). No dispara conversion a TransactionRow en el join,
            # que lo reenvuelve como q3_result (QueryResultStrategy 3).
            out_msg = serialize(
                build_batch_message(
                    "batch",
                    client=client_id,
                    msg_id=str(uuid.uuid4()),
                    batch=result,
                )
            )
            routing_key = f"worker_{self.current_downstream_worker}"
            self.current_downstream_worker = (
                self.current_downstream_worker % self.config.num_downstream_workers
            ) + 1
            self.output_mw.send(out_msg, routing_key=routing_key)

        eof_msg = serialize(
            build_eof_message(client=client_id, msg_id=str(uuid.uuid4()))
        )
        self.output_mw.send(eof_msg, routing_key="eof_broadcast")

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
