import logging
import signal
import os
from dataclasses import dataclass

from src.gateway.gateway import Gateway


@dataclass
class GatewayConfig:
    host: str
    port: int
    mom_host: str
    transactions_usd_exchange: str
    transactions_date_exchange: str
    accounts_exchange: str
    result_exchange: str
    log_level: str
    transactions_usd_workers: int
    transactions_date_workers: int
    accounts_workers: int
    # Cuantos EOF de resultado esperar antes de cerrar el cliente (1 por query
    # que corre). Con varias queries a la vez hay que esperarlas a todas.
    expected_results: int


def init_config():
    return GatewayConfig(
        host=os.getenv("SERVER_HOST", "0.0.0.0"),
        port=int(os.getenv("SERVER_PORT", 5678)),
        mom_host=os.getenv("MOM_HOST", ""),
        transactions_usd_exchange=os.getenv(
            "TRANSACTIONS_USD_EXCHANGE", "transactions_usd_exchange"
        ),
        transactions_date_exchange=os.getenv(
            "TRANSACTIONS_DATE_EXCHANGE", "transactions_date_exchange"
        ),
        accounts_exchange=os.getenv("ACCOUNTS_EXCHANGE", "accounts_exchange"),
        result_exchange=os.getenv("RESULT_EXCHANGE", "result_exchange"),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        transactions_usd_workers=int(os.getenv("TRANSACTIONS_USD_WORKERS", 1)),
        transactions_date_workers=int(os.getenv("TRANSACTIONS_DATE_WORKERS", 1)),
        accounts_workers=int(os.getenv("ACCOUNTS_WORKERS", 1)),
        expected_results=int(os.getenv("EXPECTED_RESULTS", 1)),
    )


def log_config(config: GatewayConfig):
    logging.info(
        "Gateway startup with: host=%s | port=%s | mom_host=%s | transactions_exchange=%s | accounts_exchange=%s | result_exchange=%s",
        config.host,
        config.port,
        config.mom_host,
        config.transactions_usd_exchange,
        config.accounts_exchange,
        config.result_exchange,
    )


def main():
    config = init_config()
    logging.basicConfig(level=getattr(logging, config.log_level.upper(), logging.INFO))
    logging.getLogger("pika").setLevel(logging.WARNING)
    log_config(config)

    gateway = Gateway(config)

    def handle_sigterm(signum, frame):
        logging.info("Received shutdown signal %s", signum)
        gateway.stop()

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)

    return gateway.run()


if __name__ == "__main__":
    raise SystemExit(main())
