import json
import pytest
from unittest.mock import MagicMock, patch

from src.aggregator.main import AggregatorService, AggregatorConfig
from src.aggregator.strategies import BankMaxAmountStrategy
from common.communication.internal import (
    serialize,
    build_batch_message,
    build_eof_message,
)


@pytest.fixture
def mock_middleware(monkeypatch):
    with patch(
        "src.aggregator.main.middleware.MessageMiddlewareQueueRabbitMQ"
    ) as mock_rabbit:
        yield mock_rabbit


@pytest.fixture
def service_config():
    return AggregatorConfig(
        mom_host="localhost",
        input_queue="input",
        output_queue="output",
        log_level="INFO",
        strategy=BankMaxAmountStrategy(),
    )


@pytest.fixture
def service_config_payment():
    from src.aggregator.strategies import PaymentFormatAverageStrategy

    return AggregatorConfig(
        mom_host="localhost",
        input_queue="input",
        output_queue="output",
        log_level="INFO",
        strategy=PaymentFormatAverageStrategy(),
    )


def test_aggregator_integration_process_messages(mock_middleware, service_config):
    # This integration test evaluates that AggregatorService coordinates correctly
    # with the input queues, strategies and output queues.

    # Setup service
    service = AggregatorService(service_config)

    ack_mock = MagicMock()
    nack_mock = MagicMock()

    # 1. Send data message
    batch_data = [
        {"from_bank": "BankA", "from_account": "123", "amount_paid": 100.0},
        {"from_bank": "BankA", "from_account": "456", "amount_paid": 500.0},
    ]

    data_message = build_batch_message(
        message_type="batch", client="client1", msg_id="msg-1", batch=batch_data
    )

    serialized_data_message = serialize(data_message)
    service.process_data_messsage(serialized_data_message, ack_mock, nack_mock)

    ack_mock.assert_called_once()
    service.output_queue.send.assert_not_called()  # It shouldn't send anything yet until EOF

    # 2. Send EOF message
    ack_mock.reset_mock()
    eof_message = build_eof_message(client="client1", msg_id="msg-2")

    serialized_eof_message = serialize(eof_message)
    service.process_data_messsage(serialized_eof_message, ack_mock, nack_mock)

    ack_mock.assert_called_once()

    # Verify outputs
    # Sending expected joined output using strategy
    assert service.output_queue.send.call_count == 2

    # First generated call should be the aggregated batch
    call_args_1 = service.output_queue.send.call_args_list[0][0][0]
    output_message_1 = json.loads(call_args_1)

    assert output_message_1["type"] == "batch"
    assert output_message_1["client"] == "client1"
    assert len(output_message_1["payload"]["batch"]) == 1
    assert output_message_1["payload"]["batch"][0]["amount_paid"] == 500.0

    # Second generated call should be EOF
    call_args_2 = service.output_queue.send.call_args_list[1][0][0]
    output_message_2 = json.loads(call_args_2)

    assert output_message_2["type"] == "eof"
    assert output_message_2["client"] == "client1"


def test_aggregator_integration_payment_format_average(
    mock_middleware, service_config_payment
):
    service = AggregatorService(service_config_payment)

    ack_mock = MagicMock()
    nack_mock = MagicMock()

    # 1. Send data message (Representing output from the Grouper)
    batch_data = [
        {
            "from_bank": "BankA",
            "from_account": "123",
            "payment_format": "Cash",
            "total_amount": 50.0,
            "tx_quantity": 1,
        },
        {
            "from_bank": "BankA",
            "from_account": "123",
            "payment_format": "Cash",
            "total_amount": 150.0,
            "tx_quantity": 1,
        },
        {
            "from_bank": "BankB",
            "from_account": "456",
            "payment_format": "Credit",
            "total_amount": 300.0,
            "tx_quantity": 2,
        },
    ]

    data_message = build_batch_message(
        message_type="batch", client="client_pay", msg_id="msg-p1", batch=batch_data
    )

    service.process_data_messsage(serialize(data_message), ack_mock, nack_mock)

    # 2. Send EOF message
    eof_message = build_eof_message(client="client_pay", msg_id="msg-p2")

    service.process_data_messsage(serialize(eof_message), ack_mock, nack_mock)

    assert service.output_queue.send.call_count == 2

    call_args_1 = service.output_queue.send.call_args_list[0][0][0]
    output_message_1 = json.loads(call_args_1)

    assert output_message_1["type"] == "batch"
    assert output_message_1["client"] == "client_pay"

    # Check that it correctly computed the averages using the internally combined dictionary!
    averages = output_message_1["payload"]["batch"]
    assert len(averages) == 2

    cash_res = next(r for r in averages if r["payment_format"] == "Cash")
    assert cash_res["from_bank"] == "BankA"
    assert cash_res["from_account"] == "123"
    assert cash_res["average_amount"] == 100.0  # (50+150)/(1+1)

    credit_res = next(r for r in averages if r["payment_format"] == "Credit")
    assert credit_res["from_bank"] == "BankB"
    assert credit_res["from_account"] == "456"
    assert credit_res["average_amount"] == 150.0  # 300 / 2
