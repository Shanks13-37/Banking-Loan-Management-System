PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS branches (
    branch_id INTEGER PRIMARY KEY AUTOINCREMENT,
    branch_code TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    city TEXT NOT NULL,
    ifsc_code TEXT NOT NULL UNIQUE,
    manager_name TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS customers (
    customer_id INTEGER PRIMARY KEY AUTOINCREMENT,
    full_name TEXT NOT NULL,
    phone TEXT NOT NULL UNIQUE,
    email TEXT UNIQUE,
    address TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS accounts (
    account_id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_number TEXT NOT NULL UNIQUE,
    primary_customer_id INTEGER NOT NULL REFERENCES customers(customer_id) ON DELETE RESTRICT,
    branch_id INTEGER NOT NULL REFERENCES branches(branch_id) ON DELETE RESTRICT,
    linked_account_id INTEGER REFERENCES accounts(account_id) ON DELETE SET NULL,
    account_type TEXT NOT NULL CHECK (account_type IN ('SAVINGS', 'CURRENT', 'JOINT', 'SALARY')),
    balance REAL NOT NULL DEFAULT 0 CHECK (balance >= 0),
    status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'FROZEN', 'CLOSED')),
    opened_on TEXT NOT NULL DEFAULT CURRENT_DATE,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS account_holders (
    account_id INTEGER NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    customer_id INTEGER NOT NULL REFERENCES customers(customer_id) ON DELETE CASCADE,
    holder_role TEXT NOT NULL DEFAULT 'JOINT' CHECK (holder_role IN ('PRIMARY', 'JOINT')),
    PRIMARY KEY (account_id, customer_id)
);

CREATE TABLE IF NOT EXISTS loans (
    loan_id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id INTEGER NOT NULL REFERENCES customers(customer_id) ON DELETE RESTRICT,
    account_id INTEGER NOT NULL REFERENCES accounts(account_id) ON DELETE RESTRICT,
    branch_id INTEGER NOT NULL REFERENCES branches(branch_id) ON DELETE RESTRICT,
    loan_type TEXT NOT NULL,
    principal_amount REAL NOT NULL CHECK (principal_amount > 0),
    interest_rate REAL NOT NULL CHECK (interest_rate >= 0),
    tenure_months INTEGER NOT NULL CHECK (tenure_months > 0),
    monthly_emi REAL NOT NULL CHECK (monthly_emi > 0),
    start_date TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('PENDING', 'ACTIVE', 'CLOSED', 'DEFAULTED')),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS emis (
    emi_id INTEGER PRIMARY KEY AUTOINCREMENT,
    loan_id INTEGER NOT NULL REFERENCES loans(loan_id) ON DELETE CASCADE,
    installment_no INTEGER NOT NULL,
    due_date TEXT NOT NULL,
    amount REAL NOT NULL CHECK (amount > 0),
    status TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'PAID', 'FAILED', 'OVERDUE')),
    paid_on TEXT,
    UNIQUE (loan_id, installment_no)
);

CREATE TABLE IF NOT EXISTS fixed_deposits (
    fd_id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id INTEGER NOT NULL REFERENCES customers(customer_id) ON DELETE RESTRICT,
    linked_account_id INTEGER NOT NULL REFERENCES accounts(account_id) ON DELETE RESTRICT,
    principal_amount REAL NOT NULL CHECK (principal_amount > 0),
    interest_rate REAL NOT NULL CHECK (interest_rate >= 0),
    start_date TEXT NOT NULL,
    maturity_date TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'MATURED', 'CLOSED'))
);

CREATE TABLE IF NOT EXISTS atm_logs (
    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER NOT NULL REFERENCES accounts(account_id) ON DELETE RESTRICT,
    atm_code TEXT NOT NULL,
    operation_type TEXT NOT NULL CHECK (operation_type IN ('BALANCE_INQUIRY', 'WITHDRAWAL', 'DEPOSIT', 'PIN_CHANGE')),
    amount REAL NOT NULL DEFAULT 0 CHECK (amount >= 0),
    status TEXT NOT NULL CHECK (status IN ('SUCCESS', 'FAILED')),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bank_transactions (
    txn_id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER NOT NULL REFERENCES accounts(account_id) ON DELETE RESTRICT,
    related_account_id INTEGER REFERENCES accounts(account_id) ON DELETE SET NULL,
    txn_group_id TEXT,
    txn_type TEXT NOT NULL CHECK (
        txn_type IN ('DEBIT', 'CREDIT', 'EMI', 'ATM_WITHDRAWAL', 'FD_DEPOSIT', 'LOAN_DISBURSAL')
    ),
    channel TEXT NOT NULL DEFAULT 'WEB' CHECK (channel IN ('WEB', 'ATM', 'BRANCH', 'SYSTEM')),
    amount REAL NOT NULL CHECK (amount > 0),
    balance_after REAL NOT NULL CHECK (balance_after >= 0),
    description TEXT,
    suspicious_flag INTEGER NOT NULL DEFAULT 0 CHECK (suspicious_flag IN (0, 1)),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS audit_logs (
    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_name TEXT NOT NULL,
    entity_id INTEGER NOT NULL,
    action TEXT NOT NULL,
    details TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_accounts_number ON accounts(account_number);
CREATE INDEX IF NOT EXISTS idx_accounts_branch ON accounts(branch_id);
CREATE INDEX IF NOT EXISTS idx_transactions_account_date ON bank_transactions(account_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_transactions_created_at ON bank_transactions(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_transactions_amount ON bank_transactions(amount);
CREATE INDEX IF NOT EXISTS idx_loans_customer ON loans(customer_id);
CREATE INDEX IF NOT EXISTS idx_emis_due_date ON emis(due_date);
CREATE INDEX IF NOT EXISTS idx_emis_status_due_date ON emis(status, due_date);
CREATE INDEX IF NOT EXISTS idx_atm_logs_account_time ON atm_logs(account_id, created_at DESC);

CREATE VIEW IF NOT EXISTS account_statement_view AS
SELECT
    bt.txn_id,
    bt.account_id,
    a.account_number,
    c.full_name AS customer_name,
    bt.txn_group_id,
    bt.txn_type,
    bt.channel,
    bt.amount,
    bt.balance_after,
    bt.description,
    bt.suspicious_flag,
    bt.created_at,
    ra.account_number AS related_account_number
FROM bank_transactions AS bt
JOIN accounts AS a
    ON a.account_id = bt.account_id
JOIN customers AS c
    ON c.customer_id = a.primary_customer_id
LEFT JOIN accounts AS ra
    ON ra.account_id = bt.related_account_id;

CREATE VIEW IF NOT EXISTS loan_defaulters_view AS
SELECT
    e.emi_id,
    e.loan_id,
    c.full_name AS customer_name,
    a.account_number,
    e.installment_no,
    e.due_date,
    e.amount,
    e.status,
    CAST(julianday('now') - julianday(e.due_date) AS INTEGER) AS days_overdue
FROM emis AS e
JOIN loans AS l
    ON l.loan_id = e.loan_id
JOIN customers AS c
    ON c.customer_id = l.customer_id
JOIN accounts AS a
    ON a.account_id = l.account_id
WHERE e.status IN ('PENDING', 'FAILED', 'OVERDUE')
  AND e.due_date < date('now');

CREATE VIEW IF NOT EXISTS branch_performance_view AS
SELECT
    b.branch_id,
    b.branch_code,
    b.name AS branch_name,
    b.city,
    COALESCE(account_stats.total_accounts, 0) AS total_accounts,
    COALESCE(account_stats.total_deposits, 0) AS total_deposits,
    COALESCE(loan_stats.total_loans, 0) AS total_loans,
    COALESCE(loan_stats.active_loan_portfolio, 0) AS active_loan_portfolio,
    COALESCE(fd_stats.total_fixed_deposits, 0) AS total_fixed_deposits
FROM branches AS b
LEFT JOIN (
    SELECT
        branch_id,
        COUNT(*) AS total_accounts,
        ROUND(SUM(balance), 2) AS total_deposits
    FROM accounts
    GROUP BY branch_id
) AS account_stats
    ON account_stats.branch_id = b.branch_id
LEFT JOIN (
    SELECT
        branch_id,
        COUNT(*) AS total_loans,
        ROUND(SUM(CASE WHEN status = 'ACTIVE' THEN principal_amount ELSE 0 END), 2) AS active_loan_portfolio
    FROM loans
    GROUP BY branch_id
) AS loan_stats
    ON loan_stats.branch_id = b.branch_id
LEFT JOIN (
    SELECT
        a.branch_id,
        COUNT(*) AS total_fixed_deposits
    FROM fixed_deposits AS fd
    JOIN accounts AS a
        ON a.account_id = fd.linked_account_id
    GROUP BY a.branch_id
) AS fd_stats
    ON fd_stats.branch_id = b.branch_id;

CREATE TRIGGER IF NOT EXISTS trg_transaction_audit
AFTER INSERT ON bank_transactions
FOR EACH ROW
BEGIN
    INSERT INTO audit_logs (entity_name, entity_id, action, details)
    VALUES (
        'TRANSACTION',
        NEW.txn_id,
        'CREATED',
        printf('%s %.2f on account %d via %s', NEW.txn_type, NEW.amount, NEW.account_id, NEW.channel)
    );
END;

CREATE TRIGGER IF NOT EXISTS trg_transaction_suspicious
AFTER INSERT ON bank_transactions
FOR EACH ROW
WHEN NEW.amount >= 100000
BEGIN
    UPDATE bank_transactions
    SET suspicious_flag = 1
    WHERE txn_id = NEW.txn_id;

    INSERT INTO audit_logs (entity_name, entity_id, action, details)
    VALUES (
        'TRANSACTION',
        NEW.txn_id,
        'SUSPICIOUS_FLAGGED',
        printf('Amount %.2f crossed suspicious threshold', NEW.amount)
    );
END;

CREATE TRIGGER IF NOT EXISTS trg_loan_status_audit
AFTER UPDATE OF status ON loans
FOR EACH ROW
WHEN NEW.status <> OLD.status
BEGIN
    INSERT INTO audit_logs (entity_name, entity_id, action, details)
    VALUES (
        'LOAN',
        NEW.loan_id,
        'STATUS_CHANGED',
        printf('Loan status changed from %s to %s', OLD.status, NEW.status)
    );
END;

CREATE TRIGGER IF NOT EXISTS trg_emi_paid_audit
AFTER UPDATE OF status ON emis
FOR EACH ROW
WHEN NEW.status = 'PAID' AND OLD.status <> 'PAID'
BEGIN
    INSERT INTO audit_logs (entity_name, entity_id, action, details)
    VALUES (
        'EMI',
        NEW.emi_id,
        'PAID',
        printf('Installment %d for loan %d paid', NEW.installment_no, NEW.loan_id)
    );
END;
