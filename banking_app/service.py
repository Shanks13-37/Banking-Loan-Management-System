from __future__ import annotations

import math
import threading
import time
import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Any

from .db import DEFAULT_DB_PATH, get_connection, init_db


class BankingService:
    def __init__(self, db_path: Path | str = DEFAULT_DB_PATH) -> None:
        self.db_path = Path(db_path)

    def _connect(self, *, autocommit: bool = False):
        return get_connection(self.db_path, autocommit=autocommit)

    def _fetch_all(self, connection, query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        return [dict(row) for row in connection.execute(query, params).fetchall()]

    def _fetch_one(self, connection, query: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
        row = connection.execute(query, params).fetchone()
        return dict(row) if row else None

    def _ensure_exists(self, connection, table: str, column: str, value: Any, message: str) -> dict[str, Any]:
        row = self._fetch_one(connection, f"SELECT * FROM {table} WHERE {column} = ?", (value,))
        if row is None:
            raise ValueError(message)
        return row

    def _next_account_number(self, connection, branch_id: int) -> str:
        branch = self._ensure_exists(connection, "branches", "branch_id", branch_id, "Branch not found.")
        next_serial = connection.execute("SELECT COALESCE(MAX(account_id), 0) + 1 FROM accounts").fetchone()[0]
        return f"{branch['branch_code'].replace('BR', '10')}{next_serial:04d}"

    def _record_transaction(
        self,
        connection,
        *,
        account_id: int,
        txn_type: str,
        amount: float,
        balance_after: float,
        channel: str,
        description: str,
        related_account_id: int | None = None,
        txn_group_id: str | None = None,
    ) -> int:
        cursor = connection.execute(
            """
            INSERT INTO bank_transactions (
                account_id,
                related_account_id,
                txn_group_id,
                txn_type,
                channel,
                amount,
                balance_after,
                description
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                account_id,
                related_account_id,
                txn_group_id,
                txn_type,
                channel,
                round(amount, 2),
                round(balance_after, 2),
                description,
            ),
        )
        return int(cursor.lastrowid)

    def _calculate_emi(self, principal_amount: float, interest_rate: float, tenure_months: int) -> float:
        monthly_rate = interest_rate / 1200
        if monthly_rate == 0:
            return round(principal_amount / tenure_months, 2)

        growth_factor = (1 + monthly_rate) ** tenure_months
        emi = principal_amount * monthly_rate * growth_factor / (growth_factor - 1)
        return round(emi, 2)

    def _add_months(self, original_date: date, months: int) -> date:
        year = original_date.year + (original_date.month - 1 + months) // 12
        month = (original_date.month - 1 + months) % 12 + 1
        if month == 12:
            next_month = date(year + 1, 1, 1)
        else:
            next_month = date(year, month + 1, 1)
        month_start = date(year, month, 1)
        last_day = (next_month - month_start).days
        day = min(original_date.day, last_day)
        return date(year, month, day)

    def mark_overdue_emis(self, as_of_date: str | None = None) -> int:
        effective_date = as_of_date or date.today().isoformat()
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE emis
                SET status = 'OVERDUE'
                WHERE status = 'PENDING'
                  AND due_date < ?
                """,
                (effective_date,),
            )
            connection.commit()
            return int(cursor.rowcount)

    def dashboard_summary(self) -> dict[str, Any]:
        self.mark_overdue_emis()
        with self._connect() as connection:
            metrics = dict(
                connection.execute(
                    """
                    SELECT
                        (SELECT COUNT(*) FROM customers) AS total_customers,
                        (SELECT COUNT(*) FROM accounts) AS total_accounts,
                        (SELECT ROUND(COALESCE(SUM(balance), 0), 2) FROM accounts) AS total_deposits,
                        (SELECT COUNT(*) FROM loans WHERE status = 'ACTIVE') AS active_loans,
                        (SELECT COUNT(*) FROM emis WHERE status IN ('OVERDUE', 'FAILED')) AS overdue_emis,
                        (SELECT COUNT(*) FROM bank_transactions WHERE suspicious_flag = 1) AS suspicious_transactions,
                        (SELECT COUNT(*) FROM fixed_deposits WHERE status = 'ACTIVE') AS active_fixed_deposits
                    """
                ).fetchone()
            )

            accounts = self._fetch_all(
                connection,
                """
                SELECT
                    a.account_id,
                    a.account_number,
                    a.account_type,
                    a.balance,
                    a.status,
                    a.opened_on,
                    c.full_name AS primary_customer,
                    b.branch_code,
                    b.name AS branch_name
                FROM accounts AS a
                JOIN customers AS c
                    ON c.customer_id = a.primary_customer_id
                JOIN branches AS b
                    ON b.branch_id = a.branch_id
                ORDER BY a.account_number
                """,
            )

            loans = self._fetch_all(
                connection,
                """
                SELECT
                    l.loan_id,
                    l.loan_type,
                    l.principal_amount,
                    l.interest_rate,
                    l.tenure_months,
                    l.monthly_emi,
                    l.status,
                    l.start_date,
                    c.full_name AS customer_name,
                    a.account_number
                FROM loans AS l
                JOIN customers AS c
                    ON c.customer_id = l.customer_id
                JOIN accounts AS a
                    ON a.account_id = l.account_id
                ORDER BY l.loan_id DESC
                """,
            )

            pending_emis = self._fetch_all(
                connection,
                """
                SELECT
                    e.emi_id,
                    e.loan_id,
                    e.installment_no,
                    e.due_date,
                    e.amount,
                    e.status,
                    c.full_name AS customer_name
                FROM emis AS e
                JOIN loans AS l
                    ON l.loan_id = e.loan_id
                JOIN customers AS c
                    ON c.customer_id = l.customer_id
                ORDER BY e.due_date
                LIMIT 8
                """,
            )

            emis = self._fetch_all(
                connection,
                """
                SELECT
                    e.emi_id,
                    e.loan_id,
                    e.installment_no,
                    e.due_date,
                    e.amount,
                    e.status,
                    c.full_name AS customer_name,
                    a.account_number
                FROM emis AS e
                JOIN loans AS l
                    ON l.loan_id = e.loan_id
                JOIN customers AS c
                    ON c.customer_id = l.customer_id
                JOIN accounts AS a
                    ON a.account_id = l.account_id
                ORDER BY
                    CASE e.status
                        WHEN 'OVERDUE' THEN 1
                        WHEN 'FAILED' THEN 2
                        WHEN 'PENDING' THEN 3
                        WHEN 'PAID' THEN 4
                        ELSE 5
                    END,
                    e.due_date,
                    e.emi_id
                """,
            )

            return {
                "metrics": metrics,
                "branches": self._fetch_all(connection, "SELECT * FROM branches ORDER BY branch_code"),
                "customers": self._fetch_all(connection, "SELECT * FROM customers ORDER BY full_name"),
                "accounts": accounts,
                "loans": loans,
                "pending_emis": pending_emis,
                "emis": emis,
                "recent_transactions": self._fetch_all(
                    connection,
                    """
                    SELECT *
                    FROM account_statement_view
                    ORDER BY created_at DESC, txn_id DESC
                    LIMIT 12
                    """,
                ),
                "defaulters": self._fetch_all(
                    connection,
                    """
                    SELECT *
                    FROM loan_defaulters_view
                    ORDER BY days_overdue DESC, due_date ASC
                    """,
                ),
                "branch_performance": self._fetch_all(
                    connection,
                    """
                    SELECT *
                    FROM branch_performance_view
                    ORDER BY branch_code
                    """,
                ),
                "recent_audits": self._fetch_all(
                    connection,
                    """
                    SELECT *
                    FROM audit_logs
                    ORDER BY created_at DESC, audit_id DESC
                    LIMIT 10
                    """,
                ),
            }

    def reset_demo_data(self) -> dict[str, Any]:
        init_db(self.db_path, reset=True)
        return self.dashboard_summary()

    def create_customer(self, payload: dict[str, Any]) -> dict[str, Any]:
        full_name = str(payload.get("full_name", "")).strip()
        phone = str(payload.get("phone", "")).strip()
        email = str(payload.get("email", "")).strip() or None
        address = str(payload.get("address", "")).strip()

        if not full_name or not phone or not address:
            raise ValueError("Full name, phone, and address are required.")

        with self._connect() as connection:
            try:
                cursor = connection.execute(
                    """
                    INSERT INTO customers (full_name, phone, email, address)
                    VALUES (?, ?, ?, ?)
                    """,
                    (full_name, phone, email, address),
                )
                connection.commit()
            except Exception as exc:
                connection.rollback()
                raise ValueError("Customer could not be created. Phone or email may already exist.") from exc

            return self._fetch_one(connection, "SELECT * FROM customers WHERE customer_id = ?", (cursor.lastrowid,))

    def create_account(self, payload: dict[str, Any]) -> dict[str, Any]:
        primary_customer_id = int(payload.get("primary_customer_id") or payload.get("customer_id") or 0)
        branch_id = int(payload.get("branch_id", 0))
        account_type = str(payload.get("account_type", "SAVINGS")).upper()
        opening_balance = round(float(payload.get("opening_balance") or payload.get("initial_deposit") or 0), 2)
        linked_account_id = payload.get("linked_account_id")
        joint_holder_ids = payload.get("joint_holder_ids", [])

        if account_type not in {"SAVINGS", "CURRENT", "JOINT", "SALARY"}:
            raise ValueError("Unsupported account type.")
        if opening_balance < 0:
            raise ValueError("Opening balance cannot be negative.")

        linked_account_value = int(linked_account_id) if linked_account_id not in (None, "", 0) else None
        normalized_joint_holders = {
            int(holder_id)
            for holder_id in joint_holder_ids
            if str(holder_id).strip()
        }

        with self._connect(autocommit=True) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._ensure_exists(connection, "customers", "customer_id", primary_customer_id, "Primary customer not found.")
                self._ensure_exists(connection, "branches", "branch_id", branch_id, "Branch not found.")
                if linked_account_value is not None:
                    self._ensure_exists(connection, "accounts", "account_id", linked_account_value, "Linked account not found.")

                account_number = self._next_account_number(connection, branch_id)
                cursor = connection.execute(
                    """
                    INSERT INTO accounts (
                        account_number,
                        primary_customer_id,
                        branch_id,
                        linked_account_id,
                        account_type,
                        balance
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        account_number,
                        primary_customer_id,
                        branch_id,
                        linked_account_value,
                        account_type,
                        opening_balance,
                    ),
                )
                account_id = int(cursor.lastrowid)

                connection.execute(
                    """
                    INSERT INTO account_holders (account_id, customer_id, holder_role)
                    VALUES (?, ?, 'PRIMARY')
                    """,
                    (account_id, primary_customer_id),
                )

                for joint_holder_id in normalized_joint_holders:
                    if joint_holder_id == primary_customer_id:
                        continue
                    self._ensure_exists(connection, "customers", "customer_id", joint_holder_id, "Joint holder not found.")
                    connection.execute(
                        """
                        INSERT INTO account_holders (account_id, customer_id, holder_role)
                        VALUES (?, ?, 'JOINT')
                        """,
                        (account_id, joint_holder_id),
                    )

                if opening_balance > 0:
                    self._record_transaction(
                        connection,
                        account_id=account_id,
                        txn_type="CREDIT",
                        amount=opening_balance,
                        balance_after=opening_balance,
                        channel="BRANCH",
                        description="Opening balance",
                        txn_group_id=f"OPEN-{account_id}",
                    )

                connection.commit()
            except Exception:
                connection.rollback()
                raise

            return self._fetch_one(
                connection,
                """
                SELECT
                    a.account_id,
                    a.account_number,
                    a.account_type,
                    a.balance,
                    c.full_name AS primary_customer,
                    b.branch_code
                FROM accounts AS a
                JOIN customers AS c
                    ON c.customer_id = a.primary_customer_id
                JOIN branches AS b
                    ON b.branch_id = a.branch_id
                WHERE a.account_id = ?
                """,
                (account_id,),
            )

    def transfer_funds(self, payload: dict[str, Any]) -> dict[str, Any]:
        from_account_id = int(payload.get("from_account_id", 0))
        to_account_id = int(payload.get("to_account_id", 0))
        amount = round(float(payload.get("amount", 0)), 2)
        description = str(payload.get("description", "Online transfer")).strip() or "Online transfer"

        if from_account_id == to_account_id:
            raise ValueError("Source and destination accounts must be different.")
        if amount <= 0:
            raise ValueError("Transfer amount must be positive.")

        with self._connect(autocommit=True) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                from_account = self._ensure_exists(connection, "accounts", "account_id", from_account_id, "Source account not found.")
                to_account = self._ensure_exists(connection, "accounts", "account_id", to_account_id, "Destination account not found.")

                if from_account["status"] != "ACTIVE" or to_account["status"] != "ACTIVE":
                    raise ValueError("Both accounts must be active for transfer.")

                debit_cursor = connection.execute(
                    """
                    UPDATE accounts
                    SET balance = ROUND(balance - ?, 2)
                    WHERE account_id = ?
                      AND balance >= ?
                    """,
                    (amount, from_account_id, amount),
                )
                if debit_cursor.rowcount != 1:
                    raise ValueError("Insufficient balance for transfer.")

                connection.execute(
                    """
                    UPDATE accounts
                    SET balance = ROUND(balance + ?, 2)
                    WHERE account_id = ?
                    """,
                    (amount, to_account_id),
                )

                updated_from = self._ensure_exists(connection, "accounts", "account_id", from_account_id, "Source account missing after transfer.")
                updated_to = self._ensure_exists(connection, "accounts", "account_id", to_account_id, "Destination account missing after transfer.")
                txn_group_id = f"TRF-{uuid.uuid4().hex[:10].upper()}"

                self._record_transaction(
                    connection,
                    account_id=from_account_id,
                    related_account_id=to_account_id,
                    txn_group_id=txn_group_id,
                    txn_type="DEBIT",
                    channel="WEB",
                    amount=amount,
                    balance_after=updated_from["balance"],
                    description=description,
                )
                self._record_transaction(
                    connection,
                    account_id=to_account_id,
                    related_account_id=from_account_id,
                    txn_group_id=txn_group_id,
                    txn_type="CREDIT",
                    channel="WEB",
                    amount=amount,
                    balance_after=updated_to["balance"],
                    description=description,
                )

                connection.commit()
            except Exception:
                connection.rollback()
                raise

            return {
                "txn_group_id": txn_group_id,
                "from_account_id": from_account_id,
                "to_account_id": to_account_id,
                "amount": amount,
                "from_balance": updated_from["balance"],
                "to_balance": updated_to["balance"],
            }

    def create_loan(self, payload: dict[str, Any]) -> dict[str, Any]:
        customer_id = int(payload.get("customer_id", 0))
        account_id = int(payload.get("account_id", 0))
        branch_id = int(payload.get("branch_id", 0))
        principal_amount = round(float(payload.get("principal_amount", 0)), 2)
        interest_rate = round(float(payload.get("interest_rate", 0)), 2)
        tenure_months = int(payload.get("tenure_months", 0))
        start_date = str(payload.get("start_date", date.today().isoformat()))
        loan_type = str(payload.get("loan_type", "PERSONAL")).upper()

        if principal_amount <= 0 or tenure_months <= 0:
            raise ValueError("Loan amount and tenure must be positive.")

        monthly_emi = self._calculate_emi(principal_amount, interest_rate, tenure_months)
        parsed_start_date = datetime.strptime(start_date, "%Y-%m-%d").date()

        with self._connect(autocommit=True) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._ensure_exists(connection, "customers", "customer_id", customer_id, "Customer not found.")
                account = self._ensure_exists(connection, "accounts", "account_id", account_id, "Account not found.")
                self._ensure_exists(connection, "branches", "branch_id", branch_id, "Branch not found.")

                if account["primary_customer_id"] != customer_id:
                    raise ValueError("Selected account does not belong to the selected customer.")
                if account["branch_id"] != branch_id:
                    raise ValueError("Selected account does not belong to the selected branch.")
                if account["status"] != "ACTIVE":
                    raise ValueError("Loan can only be disbursed to an active account.")

                cursor = connection.execute(
                    """
                    INSERT INTO loans (
                        customer_id,
                        account_id,
                        branch_id,
                        loan_type,
                        principal_amount,
                        interest_rate,
                        tenure_months,
                        monthly_emi,
                        start_date,
                        status
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVE')
                    """,
                    (
                        customer_id,
                        account_id,
                        branch_id,
                        loan_type,
                        principal_amount,
                        interest_rate,
                        tenure_months,
                        monthly_emi,
                        start_date,
                    ),
                )
                loan_id = int(cursor.lastrowid)

                connection.execute(
                    """
                    UPDATE accounts
                    SET balance = ROUND(balance + ?, 2)
                    WHERE account_id = ?
                    """,
                    (principal_amount, account_id),
                )
                updated_account = self._ensure_exists(connection, "accounts", "account_id", account_id, "Account not found.")
                self._record_transaction(
                    connection,
                    account_id=account_id,
                    txn_type="LOAN_DISBURSAL",
                    amount=principal_amount,
                    balance_after=updated_account["balance"],
                    channel="SYSTEM",
                    description=f"{loan_type} loan disbursal",
                    txn_group_id=f"LOAN-{loan_id}",
                )

                for installment_no in range(1, tenure_months + 1):
                    due_date = self._add_months(parsed_start_date, installment_no).isoformat()
                    connection.execute(
                        """
                        INSERT INTO emis (loan_id, installment_no, due_date, amount, status)
                        VALUES (?, ?, ?, ?, 'PENDING')
                        """,
                        (loan_id, installment_no, due_date, monthly_emi),
                    )

                connection.commit()
            except Exception:
                connection.rollback()
                raise

            return {
                "loan_id": loan_id,
                "monthly_emi": monthly_emi,
                "principal_amount": principal_amount,
                "tenure_months": tenure_months,
                "disbursed_to_account": account["account_number"],
            }

    def _close_loan_if_completed(self, connection, loan_id: int) -> None:
        pending_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM emis
            WHERE loan_id = ?
              AND status <> 'PAID'
            """,
            (loan_id,),
        ).fetchone()[0]
        if pending_count == 0:
            connection.execute(
                """
                UPDATE loans
                SET status = 'CLOSED'
                WHERE loan_id = ?
                """,
                (loan_id,),
            )

    def pay_emi(self, payload: dict[str, Any]) -> dict[str, Any]:
        emi_id = int(payload.get("emi_id", 0))
        payment_date = str(payload.get("payment_date", date.today().isoformat()))
        if emi_id <= 0:
            raise ValueError("A valid EMI id is required.")

        with self._connect(autocommit=True) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                emi = self._fetch_one(
                    connection,
                    """
                    SELECT
                        e.emi_id,
                        e.loan_id,
                        e.installment_no,
                        e.due_date,
                        e.amount,
                        e.status,
                        l.account_id
                    FROM emis AS e
                    JOIN loans AS l
                        ON l.loan_id = e.loan_id
                    WHERE e.emi_id = ?
                    """,
                    (emi_id,),
                )
                if emi is None:
                    raise ValueError("EMI not found.")
                if emi["status"] == "PAID":
                    raise ValueError("This EMI has already been paid.")

                debit_cursor = connection.execute(
                    """
                    UPDATE accounts
                    SET balance = ROUND(balance - ?, 2)
                    WHERE account_id = ?
                      AND balance >= ?
                    """,
                    (emi["amount"], emi["account_id"], emi["amount"]),
                )
                if debit_cursor.rowcount != 1:
                    raise ValueError("Insufficient balance for EMI payment.")

                connection.execute(
                    """
                    UPDATE emis
                    SET status = 'PAID',
                        paid_on = ?
                    WHERE emi_id = ?
                    """,
                    (payment_date, emi_id),
                )

                updated_account = self._ensure_exists(connection, "accounts", "account_id", emi["account_id"], "Account not found.")
                self._record_transaction(
                    connection,
                    account_id=emi["account_id"],
                    txn_type="EMI",
                    amount=emi["amount"],
                    balance_after=updated_account["balance"],
                    channel="SYSTEM",
                    description=f"EMI installment {emi['installment_no']} for loan {emi['loan_id']}",
                    txn_group_id=f"EMI-{emi['loan_id']}-{emi['installment_no']}",
                )
                self._close_loan_if_completed(connection, emi["loan_id"])
                connection.commit()
            except Exception:
                connection.rollback()
                raise

            return {
                "emi_id": emi_id,
                "loan_id": emi["loan_id"],
                "status": "PAID",
                "paid_on": payment_date,
                "remaining_balance": updated_account["balance"],
            }

    def run_emi_cycle(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = payload or {}
        run_date = str(payload.get("run_date", date.today().isoformat()))
        loan_id = int(payload.get("loan_id") or 0)
        self.mark_overdue_emis(run_date)

        with self._connect() as connection:
            params: list[Any] = [run_date]
            loan_filter = ""
            if loan_id:
                self._ensure_exists(connection, "loans", "loan_id", loan_id, "Loan not found.")
                loan_filter = " AND loan_id = ?"
                params.append(loan_id)

            due_emis = self._fetch_all(
                connection,
                f"""
                SELECT emi_id
                FROM emis
                WHERE status IN ('PENDING', 'OVERDUE', 'FAILED')
                  AND due_date <= ?
                  {loan_filter}
                ORDER BY due_date, emi_id
                """,
                tuple(params),
            )

        processed: list[dict[str, Any]] = []
        paid_count = 0
        failed_count = 0

        for record in due_emis:
            with self._connect(autocommit=True) as connection:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    emi = self._fetch_one(
                        connection,
                        """
                        SELECT
                            e.emi_id,
                            e.loan_id,
                            e.installment_no,
                            e.due_date,
                            e.amount,
                            e.status,
                            l.account_id
                        FROM emis AS e
                        JOIN loans AS l
                            ON l.loan_id = e.loan_id
                        WHERE e.emi_id = ?
                        """,
                        (record["emi_id"],),
                    )
                    if emi is None or emi["status"] == "PAID":
                        connection.rollback()
                        continue

                    debit_cursor = connection.execute(
                        """
                        UPDATE accounts
                        SET balance = ROUND(balance - ?, 2)
                        WHERE account_id = ?
                          AND balance >= ?
                        """,
                        (emi["amount"], emi["account_id"], emi["amount"]),
                    )

                    if debit_cursor.rowcount == 1:
                        connection.execute(
                            """
                            UPDATE emis
                            SET status = 'PAID',
                                paid_on = ?
                            WHERE emi_id = ?
                            """,
                            (run_date, emi["emi_id"]),
                        )
                        updated_account = self._ensure_exists(
                            connection,
                            "accounts",
                            "account_id",
                            emi["account_id"],
                            "Account not found.",
                        )
                        self._record_transaction(
                            connection,
                            account_id=emi["account_id"],
                            txn_type="EMI",
                            amount=emi["amount"],
                            balance_after=updated_account["balance"],
                            channel="SYSTEM",
                            description=f"Auto debit EMI {emi['installment_no']} for loan {emi['loan_id']}",
                            txn_group_id=f"AUTO-EMI-{emi['loan_id']}-{emi['installment_no']}",
                        )
                        self._close_loan_if_completed(connection, emi["loan_id"])
                        connection.commit()
                        paid_count += 1
                        processed.append({"emi_id": emi["emi_id"], "loan_id": emi["loan_id"], "status": "PAID"})
                    else:
                        failed_status = "FAILED" if emi["due_date"] == run_date else "OVERDUE"
                        connection.execute(
                            """
                            UPDATE emis
                            SET status = ?
                            WHERE emi_id = ?
                            """,
                            (failed_status, emi["emi_id"]),
                        )
                        connection.commit()
                        failed_count += 1
                        processed.append(
                            {
                                "emi_id": emi["emi_id"],
                                "loan_id": emi["loan_id"],
                                "status": failed_status,
                                "message": "Insufficient balance for auto-debit.",
                            }
                        )
                except Exception:
                    connection.rollback()
                    raise

        return {
            "run_date": run_date,
            "loan_id": loan_id or None,
            "processed": processed,
            "paid_count": paid_count,
            "failed_count": failed_count,
            "paid": paid_count,
            "failed": failed_count,
        }

    def get_account_statement(self, account_id: int, *, limit: int = 25) -> list[dict[str, Any]]:
        if account_id <= 0:
            raise ValueError("A valid account id is required.")
        with self._connect() as connection:
            return self._fetch_all(
                connection,
                """
                SELECT *
                FROM account_statement_view
                WHERE account_id = ?
                ORDER BY created_at DESC, txn_id DESC
                LIMIT ?
                """,
                (account_id, limit),
            )

    def get_defaulters(self) -> list[dict[str, Any]]:
        self.mark_overdue_emis()
        with self._connect() as connection:
            return self._fetch_all(
                connection,
                """
                SELECT *
                FROM loan_defaulters_view
                ORDER BY days_overdue DESC, due_date ASC
                """,
            )

    def get_branch_performance(self) -> list[dict[str, Any]]:
        with self._connect() as connection:
            return self._fetch_all(
                connection,
                """
                SELECT *
                FROM branch_performance_view
                ORDER BY branch_code
                """,
            )

    def _prepare_concurrency_account(self) -> int:
        with self._connect(autocommit=True) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                demo_account = self._fetch_one(
                    connection,
                    """
                    SELECT account_id
                    FROM accounts
                    WHERE account_number = '99990001'
                    """,
                )
                if demo_account is None:
                    raise ValueError("Concurrency demo account is missing from seed data.")

                account_id = int(demo_account["account_id"])
                connection.execute(
                    """
                    UPDATE accounts
                    SET balance = 1500
                    WHERE account_id = ?
                    """,
                    (account_id,),
                )
                connection.execute(
                    """
                    DELETE FROM bank_transactions
                    WHERE account_id = ?
                      AND description LIKE 'Concurrency demo%'
                    """,
                    (account_id,),
                )
                connection.execute(
                    """
                    DELETE FROM atm_logs
                    WHERE account_id = ?
                      AND atm_code LIKE 'RACE-%'
                    """,
                    (account_id,),
                )
                connection.commit()
                return account_id
            except Exception:
                connection.rollback()
                raise

    def simulate_concurrency(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = payload or {}
        amount = round(float(payload.get("amount", 1000)), 2)
        if amount <= 0:
            raise ValueError("Withdrawal amount must be positive.")

        demo_account_id = self._prepare_concurrency_account()

        def read_balance() -> float:
            with self._connect() as connection:
                return float(
                    connection.execute(
                        "SELECT balance FROM accounts WHERE account_id = ?",
                        (demo_account_id,),
                    ).fetchone()[0]
                )

        def naive_withdraw(label: str, gate: threading.Barrier, results: list[dict[str, Any]]) -> None:
            connection = self._connect(autocommit=True)
            try:
                opening_balance = float(
                    connection.execute(
                        "SELECT balance FROM accounts WHERE account_id = ?",
                        (demo_account_id,),
                    ).fetchone()[0]
                )
                gate.wait()
                time.sleep(0.2)
                if opening_balance >= amount:
                    new_balance = round(opening_balance - amount, 2)
                    connection.execute(
                        """
                        UPDATE accounts
                        SET balance = ?
                        WHERE account_id = ?
                        """,
                        (new_balance, demo_account_id),
                    )
                    connection.execute(
                        """
                        INSERT INTO bank_transactions (
                            account_id,
                            txn_group_id,
                            txn_type,
                            channel,
                            amount,
                            balance_after,
                            description
                        )
                        VALUES (?, ?, 'ATM_WITHDRAWAL', 'ATM', ?, ?, ?)
                        """,
                        (
                            demo_account_id,
                            f"RACE-NAIVE-{label}",
                            amount,
                            new_balance,
                            f"Concurrency demo naive withdrawal {label}",
                        ),
                    )
                    connection.execute(
                        """
                        INSERT INTO atm_logs (account_id, atm_code, operation_type, amount, status)
                        VALUES (?, ?, 'WITHDRAWAL', ?, 'SUCCESS')
                        """,
                        (demo_account_id, f"RACE-NAIVE-{label}", amount),
                    )
                    results.append({"worker": label, "status": "SUCCESS", "balance_seen": opening_balance})
                else:
                    results.append({"worker": label, "status": "FAILED", "balance_seen": opening_balance})
            finally:
                connection.close()

        def safe_withdraw(label: str, gate: threading.Barrier, results: list[dict[str, Any]]) -> None:
            connection = self._connect(autocommit=True)
            try:
                gate.wait()
                connection.execute("BEGIN IMMEDIATE")
                opening_balance = float(
                    connection.execute(
                        "SELECT balance FROM accounts WHERE account_id = ?",
                        (demo_account_id,),
                    ).fetchone()[0]
                )
                time.sleep(0.2)
                debit_cursor = connection.execute(
                    """
                    UPDATE accounts
                    SET balance = ROUND(balance - ?, 2)
                    WHERE account_id = ?
                      AND balance >= ?
                    """,
                    (amount, demo_account_id, amount),
                )
                if debit_cursor.rowcount == 1:
                    updated_balance = float(
                        connection.execute(
                            "SELECT balance FROM accounts WHERE account_id = ?",
                            (demo_account_id,),
                        ).fetchone()[0]
                    )
                    connection.execute(
                        """
                        INSERT INTO bank_transactions (
                            account_id,
                            txn_group_id,
                            txn_type,
                            channel,
                            amount,
                            balance_after,
                            description
                        )
                        VALUES (?, ?, 'ATM_WITHDRAWAL', 'ATM', ?, ?, ?)
                        """,
                        (
                            demo_account_id,
                            f"RACE-SAFE-{label}",
                            amount,
                            updated_balance,
                            f"Concurrency demo safe withdrawal {label}",
                        ),
                    )
                    connection.execute(
                        """
                        INSERT INTO atm_logs (account_id, atm_code, operation_type, amount, status)
                        VALUES (?, ?, 'WITHDRAWAL', ?, 'SUCCESS')
                        """,
                        (demo_account_id, f"RACE-SAFE-{label}", amount),
                    )
                    connection.commit()
                    results.append({"worker": label, "status": "SUCCESS", "balance_seen": opening_balance})
                else:
                    connection.rollback()
                    results.append({"worker": label, "status": "FAILED", "balance_seen": opening_balance})
            except Exception as exc:
                connection.rollback()
                results.append({"worker": label, "status": "ERROR", "error": str(exc)})
            finally:
                connection.close()

        def run_race(mode: str) -> dict[str, Any]:
            self._prepare_concurrency_account()
            starting_balance = read_balance()
            gate = threading.Barrier(2)
            results: list[dict[str, Any]] = []
            worker = naive_withdraw if mode == "naive" else safe_withdraw

            threads = [
                threading.Thread(target=worker, args=("A", gate, results), daemon=True),
                threading.Thread(target=worker, args=("B", gate, results), daemon=True),
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

            ending_balance = read_balance()
            successful_withdrawals = sum(1 for item in results if item["status"] == "SUCCESS")
            ledger_balance = round(starting_balance - (successful_withdrawals * amount), 2)
            return {
                "starting_balance": starting_balance,
                "withdraw_amount": amount,
                "results": sorted(results, key=lambda item: item["worker"]),
                "successful_withdrawals": successful_withdrawals,
                "final_balance": ending_balance,
                "ledger_balance_from_successful_events": ledger_balance,
                "lost_update_detected": not math.isclose(ending_balance, ledger_balance, rel_tol=0, abs_tol=0.001),
            }

        naive_result = run_race("naive")
        safe_result = run_race("safe")

        return {
            "account_id": demo_account_id,
            "naive": {
                **naive_result,
                "explanation": "Both threads read the same balance first, so the second update overwrites the first one.",
            },
            "safe": {
                **safe_result,
                "explanation": "BEGIN IMMEDIATE serializes the withdrawals, so only one debit succeeds.",
            },
        }
