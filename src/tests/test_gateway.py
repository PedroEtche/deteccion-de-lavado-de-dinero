import unittest

from src.communication.protocols.queue_protocol.internal import TransactionRow
from src.gateway.main import _dict_to_transaction_row


class TestDictToTransactionRow(unittest.TestCase):

    def test_full_row_maps_all_fields(self):
        row = {
            "Timestamp": "2022/09/02 06:00",
            "From Bank": "20",
            "Account": "A",
            "To Bank": "30",
            "Account.1": "B",
            "Amount Received": "100.5",
            "Receiving Currency": "USD",
            "Amount Paid": "100.5",
            "Payment Currency": "USD",
            "Payment Format": "Wire",
        }
        tx = _dict_to_transaction_row(row)
        self.assertIsInstance(tx, TransactionRow)
        self.assertEqual(tx.timestamp, "2022/09/02 06:00")
        self.assertEqual(tx.from_bank, "20")
        self.assertEqual(tx.from_account, "A")
        self.assertEqual(tx.to_bank, "30")
        self.assertEqual(tx.to_account, "B")
        self.assertEqual(tx.amount_received, 100.5)
        self.assertEqual(tx.receiving_currency, "USD")
        self.assertEqual(tx.amount_paid, 100.5)
        self.assertEqual(tx.payment_currency, "USD")
        self.assertEqual(tx.payment_format, "Wire")

    def test_amount_fields_are_parsed_as_float(self):
        row = {"Amount Paid": "49.99", "Amount Received": "50.0"}
        tx = _dict_to_transaction_row(row)
        self.assertIsInstance(tx.amount_paid, float)
        self.assertEqual(tx.amount_paid, 49.99)
        self.assertEqual(tx.amount_received, 50.0)

    def test_missing_csv_fields_become_none(self):
        row = {"From Bank": "20"}
        tx = _dict_to_transaction_row(row)
        self.assertEqual(tx.from_bank, "20")
        self.assertIsNone(tx.timestamp)
        self.assertIsNone(tx.amount_paid)
        self.assertIsNone(tx.payment_currency)

    def test_empty_string_values_become_none(self):
        row = {"From Bank": "20", "Amount Paid": "", "Timestamp": ""}
        tx = _dict_to_transaction_row(row)
        self.assertEqual(tx.from_bank, "20")
        self.assertIsNone(tx.amount_paid)
        self.assertIsNone(tx.timestamp)

    def test_unknown_csv_fields_are_ignored(self):
        row = {"From Bank": "20", "Is Laundering": "1", "Unknown": "value"}
        tx = _dict_to_transaction_row(row)
        self.assertEqual(tx.from_bank, "20")

    def test_account_dot_one_maps_to_to_account(self):
        row = {"Account": "A", "Account.1": "B"}
        tx = _dict_to_transaction_row(row)
        self.assertEqual(tx.from_account, "A")
        self.assertEqual(tx.to_account, "B")


if __name__ == "__main__":
    unittest.main()
