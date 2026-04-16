# Banking & Loan Management System

A database-focused mini core-banking project built for a DBMS course. The application is intentionally centered on concepts you can explain in a presentation or viva:

- ACID-compliant fund transfers
- concurrency control with a lost-update demo and locking fix
- normalized schema with customers, accounts, loans, EMIs, branches, fixed deposits, ATM logs, and audit logs
- triggers, views, and indexes
- a web dashboard for live interaction

The project runs with Python's standard library plus SQLite, so there are no external package installs required.

## Features Mapped To DBMS Concepts

### 1. Atomic fund transfer

Transfers debit one account and credit another inside a single transaction. If any step fails, the full transfer is rolled back.

### 2. Concurrency control demo

The dashboard includes a side-by-side demo:

- unsafe withdrawal flow: two threads read the same balance and overwrite each other
- safe withdrawal flow: `BEGIN IMMEDIATE` serializes writes so only one withdrawal succeeds

This gives you a concrete lost-update example for demonstration.

### 3. Normalized relational model

Main entities:

- `customers`
- `branches`
- `accounts`
- `account_holders`
- `bank_transactions`
- `loans`
- `emis`
- `fixed_deposits`
- `atm_logs`
- `audit_logs`

Design highlights:

- `emis` behaves like a weak entity because it depends on `loans`
- `accounts.linked_account_id` provides a recursive relationship example
- `account_holders` supports joint account ownership cleanly

### 4. Triggers

Triggers automatically:

- insert audit trail rows for transactions
- flag transactions above INR 100000 as suspicious
- log EMI payment and loan status changes

### 5. Views

- `account_statement_view`
- `loan_defaulters_view`
- `branch_performance_view`

### 6. Indexing

Indexes are included for:

- account lookup by account number
- statement generation by account and transaction date
- EMI due-date searches
- ATM log lookups

## Project Structure

```text
.
|-- app.py
|-- banking_app/
|   |-- __init__.py
|   |-- db.py
|   `-- service.py
|-- database/
|   |-- banking.db
|   |-- postgres_reference.sql
|   |-- schema.sql
|   `-- seed.sql
|-- static/
|   `-- index.html
`-- tests/
    `-- test_service.py
```

## How To Run

Initialize the database and start the web app:

```bash
python3 app.py --reset-db
```

Then open:

```text
http://127.0.0.1:8000
```

## How To Run Tests

```bash
python3 -m unittest discover -s tests
```

## Suggested Demo Flow For Presentation

1. Open the dashboard and show seeded accounts, loans, pending EMIs, and reports.
2. Perform a fund transfer and explain atomicity and rollback.
3. Run the EMI cycle and show how overdue or paid installments change state.
4. Open the loan defaulters and branch performance views.
5. Run the built-in concurrency demo from the dashboard and compare unsafe vs safe outcomes.
6. Point to the audit log and suspicious-transaction flagging.

## Notes For Viva / Submission

- The app uses SQLite because it is easy to run in any environment.
- For theory discussion around PostgreSQL row-level locking and `SELECT ... FOR UPDATE`, see [database/postgres_reference.sql](database/postgres_reference.sql).
- If your faculty asks about scheduled EMI deduction, explain that the app exposes a manual "Run EMI Cycle" admin action; in production this would be scheduled through a database job or cron-based worker.
