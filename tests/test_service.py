from __future__ import annotations

import tempfile
import unittest
from datetime import date
from pathlib import Path

from banking_app import BankingService, init_db


class BankingServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "banking_test.db"
        init_db(self.db_path, reset=True)
        self.service = BankingService(self.db_path)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_transfer_is_atomic_and_records_both_entries(self) -> None:
        result = self.service.transfer_funds(
            {
                "from_account_id": 1,
                "to_account_id": 2,
                "amount": 500,
                "description": "Unit test transfer",
            }
        )

        with self.service._connect() as connection:
            from_balance = connection.execute(
                "SELECT balance FROM accounts WHERE account_id = 1"
            ).fetchone()[0]
            to_balance = connection.execute(
                "SELECT balance FROM accounts WHERE account_id = 2"
            ).fetchone()[0]
            entry_count = connection.execute(
                """
                SELECT COUNT(*)
                FROM bank_transactions
                WHERE txn_group_id = ?
                """,
                (result["txn_group_id"],),
            ).fetchone()[0]

        self.assertEqual(from_balance, 94500)
        self.assertEqual(to_balance, 42500)
        self.assertEqual(entry_count, 2)

    def test_transfer_rolls_back_on_insufficient_balance(self) -> None:
        with self.assertRaises(ValueError):
            self.service.transfer_funds(
                {
                    "from_account_id": 3,
                    "to_account_id": 2,
                    "amount": 10000,
                    "description": "Should fail",
                }
            )

        with self.service._connect() as connection:
            from_balance = connection.execute(
                "SELECT balance FROM accounts WHERE account_id = 3"
            ).fetchone()[0]
            to_balance = connection.execute(
                "SELECT balance FROM accounts WHERE account_id = 2"
            ).fetchone()[0]

        self.assertEqual(from_balance, 6800)
        self.assertEqual(to_balance, 42000)

    def test_create_loan_generates_emi_schedule_and_disburses_amount(self) -> None:
        result = self.service.create_loan(
            {
                "customer_id": 3,
                "account_id": 3,
                "branch_id": 3,
                "principal_amount": 120000,
                "interest_rate": 12,
                "tenure_months": 6,
                "start_date": "2026-04-16",
                "loan_type": "EDUCATION",
            }
        )

        with self.service._connect() as connection:
            emi_count = connection.execute(
                "SELECT COUNT(*) FROM emis WHERE loan_id = ?",
                (result["loan_id"],),
            ).fetchone()[0]
            account_balance = connection.execute(
                "SELECT balance FROM accounts WHERE account_id = 3"
            ).fetchone()[0]

        self.assertEqual(emi_count, 6)
        self.assertEqual(account_balance, 126800)
        self.assertGreater(result["monthly_emi"], 0)

    def test_create_account_accepts_ui_field_aliases(self) -> None:
        result = self.service.create_account(
            {
                "customer_id": 4,
                "branch_id": 1,
                "account_type": "SAVINGS",
                "initial_deposit": 2500,
            }
        )

        with self.service._connect() as connection:
            balance = connection.execute(
                "SELECT balance FROM accounts WHERE account_id = ?",
                (result["account_id"],),
            ).fetchone()[0]

        self.assertEqual(result["primary_customer"], "Naina Desai")
        self.assertEqual(balance, 2500)

    def test_run_emi_cycle_can_target_a_single_loan(self) -> None:
        result = self.service.run_emi_cycle({"loan_id": 1, "run_date": date.today().isoformat()})

        with self.service._connect() as connection:
            other_loan_status = connection.execute(
                "SELECT status FROM emis WHERE loan_id = 2 AND installment_no = 2"
            ).fetchone()[0]

        self.assertEqual(result["loan_id"], 1)
        self.assertEqual(result["paid"], result["paid_count"])
        self.assertEqual(result["failed"], result["failed_count"])
        self.assertTrue(result["processed"])
        self.assertTrue(all(item["loan_id"] == 1 for item in result["processed"]))
        self.assertEqual(other_loan_status, "PENDING")

    def test_init_db_reset_handles_existing_sqlite_sidecar_files(self) -> None:
        wal_path = self.db_path.with_name(f"{self.db_path.name}-wal")
        shm_path = self.db_path.with_name(f"{self.db_path.name}-shm")
        wal_path.write_text("stale wal", encoding="utf-8")
        shm_path.write_text("stale shm", encoding="utf-8")

        init_db(self.db_path, reset=True)
        self.service = BankingService(self.db_path)
        summary = self.service.dashboard_summary()

        self.assertIn("metrics", summary)
        self.assertTrue(self.db_path.exists())

    def test_concurrency_demo_shows_naive_race_and_safe_serialization(self) -> None:
        result = self.service.simulate_concurrency({"amount": 1000})

        self.assertEqual(result["naive"]["successful_withdrawals"], 2)
        self.assertTrue(result["naive"]["lost_update_detected"])
        self.assertEqual(result["naive"]["final_balance"], 500)
        self.assertEqual(result["safe"]["successful_withdrawals"], 1)
        self.assertFalse(result["safe"]["lost_update_detected"])
        self.assertEqual(result["safe"]["final_balance"], 500)


if __name__ == "__main__":
    unittest.main()
