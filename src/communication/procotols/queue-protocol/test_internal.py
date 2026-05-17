import pytest
import json
import uuid
from internal import (
    build_message,
    build_batch_message,
    build_raw_transactions_message,
    build_raw_accounts_message,
    build_eof_message,
    serialize,
    deserialize,
    TransactionRow,
    AccountRow,
    MessageValidationError,
)


class TestMessageBuilding:
    """Test construction of different message types"""

    @pytest.fixture
    def client_id(self):
        return str(uuid.uuid4())

    @pytest.fixture
    def msg_id(self):
        return str(uuid.uuid4())

    def test_build_eof_message(self, client_id, msg_id):
        """Test building EOF message"""
        message = build_eof_message(client=client_id, msg_id=msg_id)
        
        assert message["type"] == "eof"
        assert message["client"] == client_id
        assert message["msg_id"] == msg_id
        assert "payload" not in message

    def test_build_raw_transactions_message(self, client_id, msg_id):
        """Test building raw_transactions message with example data"""
        transactions = [
            TransactionRow(
                timestamp="2022/09/02 06:00",
                from_bank=20,
                from_account="802EABEB0",
                to_bank=220270,
                to_account="80E25DFF0",
                amount_received=9661.410000,
                receiving_currency="USD",
                amount_paid=9661.410000,
                payment_currency="USD",
                payment_format="WIRE",
            ),
            TransactionRow(
                timestamp="2022/09/02 07:00",
                from_bank=30,
                from_account="802EABEB1",
                to_bank=220271,
                to_account="80E25DFF1",
                amount_received=5000.00,
                receiving_currency="EUR",
                amount_paid=5000.00,
                payment_currency="EUR",
                payment_format="TRANSFER",
            ),
        ]
        
        message = build_raw_transactions_message(
            client=client_id,
            msg_id=msg_id,
            batch=transactions,
        )
        
        assert message["type"] == "raw_transactions"
        assert message["client"] == client_id
        assert message["msg_id"] == msg_id
        assert message["payload"]["batch_size"] == 2
        assert len(message["payload"]["batch"]) == 2
        assert message["payload"]["batch"] == transactions

    def test_build_raw_accounts_message(self, client_id, msg_id):
        """Test building raw_accounts message with example data"""
        accounts = [
            AccountRow(
                bank_name="China Bank #2820",
                bank_id=314693,
                account_number="81B86A280",
                entity_id="800D8CCF0",
                entity_name="Corporation #41344",
            ),
            AccountRow(
                bank_name="Global Bank #5000",
                bank_id=500000,
                account_number="81B86A281",
                entity_id="800D8CCF1",
                entity_name="Corporation #41345",
            ),
        ]
        
        message = build_raw_accounts_message(
            client=client_id,
            msg_id=msg_id,
            batch=accounts,
        )
        
        assert message["type"] == "raw_accounts"
        assert message["client"] == client_id
        assert message["msg_id"] == msg_id
        assert message["payload"]["batch_size"] == 2
        assert len(message["payload"]["batch"]) == 2
        assert message["payload"]["batch"] == accounts

class TestMessageSerialization:
    """Test serialization and deserialization of messages"""

    @pytest.fixture
    def client_id(self):
        return str(uuid.uuid4())

    @pytest.fixture
    def msg_id(self):
        return str(uuid.uuid4())

    def test_serialize_deserialize_raw_transactions(self, client_id, msg_id):
        """Test full cycle: build -> serialize -> deserialize"""
        transactions = [
            TransactionRow(
                timestamp="2022/09/02 06:00",
                from_bank=20,
                from_account="802EABEB0",
                to_bank=220270,
                to_account="80E25DFF0",
                amount_received=9661.410000,
                receiving_currency="USD",
                amount_paid=9661.410000,
                payment_currency="USD",
                payment_format="WIRE",
            )
        ]
        
        original = build_raw_transactions_message(
            client=client_id,
            msg_id=msg_id,
            batch=transactions,
        )
        
        serialized = serialize(original)
        deserialized = deserialize(serialized)
        
        assert deserialized == original

    def test_serialize_deserialize_raw_accounts(self, client_id, msg_id):
        """Test full cycle for raw_accounts"""
        accounts = [
            AccountRow(
                bank_name="China Bank #2820",
                bank_id=314693,
                account_number="81B86A280",
                entity_id="800D8CCF0",
                entity_name="Corporation #41344",
            ),
        ]
        
        original = build_raw_accounts_message(
            client=client_id,
            msg_id=msg_id,
            batch=accounts,
        )
        
        serialized = serialize(original)
        deserialized = deserialize(serialized)
        
        assert deserialized == original


class TestValidation:
    """Test validation logic"""

    @pytest.fixture
    def client_id(self):
        return str(uuid.uuid4())

    @pytest.fixture
    def msg_id(self):
        return str(uuid.uuid4())

    def test_missing_type_field(self, client_id, msg_id):
        """Test that message with None type fails validation"""
        with pytest.raises(MessageValidationError, match="non-empty string"):
            build_message(None, client=client_id, msg_id=msg_id)

    def test_empty_type_string(self, client_id, msg_id):
        """Test that empty type string fails validation"""
        with pytest.raises(MessageValidationError, match="non-empty string"):
            build_message("", client=client_id, msg_id=msg_id)


class TestDeserialization:
    """Test deserialization edge cases and error handling"""
    def test_deserialize_invalid_schema(self):
        invalid_schema = json.dumps({
            "type": "raw_transactions",
            "client": str(uuid.uuid4()),
            "msg_id": str(uuid.uuid4()),
            # missing payload for raw_transactions
        }).encode("utf-8")
        
        with pytest.raises(MessageValidationError, match="missing 'payload'"):
            deserialize(invalid_schema)


class TestEdgeCases:
    """Test edge cases and special scenarios"""

    @pytest.fixture
    def client_id(self):
        return str(uuid.uuid4())

    @pytest.fixture
    def msg_id(self):
        return str(uuid.uuid4())

    def test_empty_batch(self, client_id, msg_id):
        """Test batch message with empty batch"""
        message = build_raw_transactions_message(
            client=client_id,
            msg_id=msg_id,
            batch=[],
        )
        
        assert message["payload"]["batch_size"] == 0
        assert message["payload"]["batch"] == []


class TestIntegration:
    """Integration tests with realistic scenarios"""

    @pytest.fixture
    def client_id(self):
        return str(uuid.uuid4())

    @pytest.fixture
    def msg_ids(self):
        return [str(uuid.uuid4()) for _ in range(3)]

    def test_full_transaction_flow(self, client_id, msg_ids):
        """Test a complete transaction data flow"""
        # Batch 1: Raw transactions
        batch1_transactions = [
            TransactionRow(
                timestamp="2022/09/02 06:00",
                from_bank=20,
                from_account="802EABEB0",
                to_bank=220270,
                to_account="80E25DFF0",
                amount_received=9661.410000,
                receiving_currency="USD",
                amount_paid=9661.410000,
                payment_currency="USD",
                payment_format="WIRE",
            )
        ]

        msg1 = build_raw_transactions_message(
            client=client_id,
            msg_id=msg_ids[0],
            batch=batch1_transactions,
        )
        # Batch 2: Raw accounts

        batch2_accounts = [
            AccountRow(
                bank_name="China Bank #2820",
                bank_id=314693,
                account_number="81B86A280",
                entity_id="800D8CCF0",
                entity_name="Corporation #41344",
            )
        ]

        msg2 = build_raw_accounts_message(
            client=client_id,
            msg_id=msg_ids[1],
            batch=batch2_accounts,
        )

        # EOF message
        msg3 = build_eof_message(client=client_id, msg_id=msg_ids[2])

        # Serialize all
        serialized1 = serialize(msg1)
        serialized2 = serialize(msg2)
        serialized3 = serialize(msg3)

        # Deserialize all
        deserialized1 = deserialize(serialized1)
        deserialized2 = deserialize(serialized2)
        deserialized3 = deserialize(serialized3)

        # Verify
        assert deserialized1 == msg1
        assert deserialized2 == msg2
        assert deserialized3 == msg3
        assert deserialized1["client"] == client_id
        assert deserialized2["client"] == client_id
        assert deserialized3["client"] == client_id
