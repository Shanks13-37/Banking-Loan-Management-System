-- Reference snippets for viva / theory discussion.
-- The main runnable project uses SQLite for zero-dependency setup.
-- If you want to explain row-level locking exactly as in PostgreSQL,
-- use the following transfer and concurrency example.

BEGIN;

SELECT account_id, balance
FROM accounts
WHERE account_id IN (1, 2)
FOR UPDATE;

UPDATE accounts
SET balance = balance - 500
WHERE account_id = 1
  AND balance >= 500;

UPDATE accounts
SET balance = balance + 500
WHERE account_id = 2;

COMMIT;

-- Concurrency demo:
-- Session A:
BEGIN;
SELECT balance FROM accounts WHERE account_id = 1 FOR UPDATE;
-- Keep the transaction open.

-- Session B:
BEGIN;
SELECT balance FROM accounts WHERE account_id = 1 FOR UPDATE;
-- This waits until Session A commits or rolls back.
