INSERT INTO branches (branch_code, name, city, ifsc_code, manager_name)
VALUES
    ('BR001', 'Central Finance Hub', 'Pune', 'BLMS0001001', 'Aditi Rao'),
    ('BR002', 'Riverside Banking Centre', 'Mumbai', 'BLMS0002002', 'Harsh Menon'),
    ('BR003', 'Heritage Loan Desk', 'Nashik', 'BLMS0003003', 'Meera Joshi');

INSERT INTO customers (full_name, phone, email, address)
VALUES
    ('Aarav Kulkarni', '9000000001', 'aarav@example.com', 'Baner, Pune'),
    ('Diya Mehta', '9000000002', 'diya@example.com', 'Powai, Mumbai'),
    ('Karan Shah', '9000000003', 'karan@example.com', 'College Road, Nashik'),
    ('Naina Desai', '9000000004', 'naina@example.com', 'Kothrud, Pune'),
    ('Operations Demo', '9000000005', 'ops@example.com', 'Internal Demo Account');

INSERT INTO accounts (
    account_number,
    primary_customer_id,
    branch_id,
    linked_account_id,
    account_type,
    balance,
    status,
    opened_on
)
VALUES
    (
        '10010001',
        (SELECT customer_id FROM customers WHERE phone = '9000000001'),
        (SELECT branch_id FROM branches WHERE branch_code = 'BR001'),
        NULL,
        'SAVINGS',
        95000,
        'ACTIVE',
        date('now', '-240 day')
    ),
    (
        '10010002',
        (SELECT customer_id FROM customers WHERE phone = '9000000002'),
        (SELECT branch_id FROM branches WHERE branch_code = 'BR002'),
        NULL,
        'CURRENT',
        42000,
        'ACTIVE',
        date('now', '-180 day')
    ),
    (
        '10010003',
        (SELECT customer_id FROM customers WHERE phone = '9000000003'),
        (SELECT branch_id FROM branches WHERE branch_code = 'BR003'),
        NULL,
        'SAVINGS',
        6800,
        'ACTIVE',
        date('now', '-110 day')
    ),
    (
        '10010004',
        (SELECT customer_id FROM customers WHERE phone = '9000000001'),
        (SELECT branch_id FROM branches WHERE branch_code = 'BR001'),
        NULL,
        'JOINT',
        54000,
        'ACTIVE',
        date('now', '-75 day')
    ),
    (
        '99990001',
        (SELECT customer_id FROM customers WHERE phone = '9000000005'),
        (SELECT branch_id FROM branches WHERE branch_code = 'BR002'),
        NULL,
        'SAVINGS',
        1500,
        'ACTIVE',
        date('now', '-1 day')
    );

UPDATE accounts
SET linked_account_id = (
    SELECT account_id
    FROM accounts
    WHERE account_number = '10010001'
)
WHERE account_number = '10010004';

INSERT INTO account_holders (account_id, customer_id, holder_role)
VALUES
    (
        (SELECT account_id FROM accounts WHERE account_number = '10010001'),
        (SELECT customer_id FROM customers WHERE phone = '9000000001'),
        'PRIMARY'
    ),
    (
        (SELECT account_id FROM accounts WHERE account_number = '10010002'),
        (SELECT customer_id FROM customers WHERE phone = '9000000002'),
        'PRIMARY'
    ),
    (
        (SELECT account_id FROM accounts WHERE account_number = '10010003'),
        (SELECT customer_id FROM customers WHERE phone = '9000000003'),
        'PRIMARY'
    ),
    (
        (SELECT account_id FROM accounts WHERE account_number = '10010004'),
        (SELECT customer_id FROM customers WHERE phone = '9000000001'),
        'PRIMARY'
    ),
    (
        (SELECT account_id FROM accounts WHERE account_number = '10010004'),
        (SELECT customer_id FROM customers WHERE phone = '9000000004'),
        'JOINT'
    ),
    (
        (SELECT account_id FROM accounts WHERE account_number = '99990001'),
        (SELECT customer_id FROM customers WHERE phone = '9000000005'),
        'PRIMARY'
    );

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
VALUES
    (
        (SELECT account_id FROM accounts WHERE account_number = '10010001'),
        NULL,
        'SEED-OPEN-1',
        'CREDIT',
        'BRANCH',
        95000,
        95000,
        'Opening balance'
    ),
    (
        (SELECT account_id FROM accounts WHERE account_number = '10010002'),
        NULL,
        'SEED-OPEN-2',
        'CREDIT',
        'BRANCH',
        42000,
        42000,
        'Opening balance'
    ),
    (
        (SELECT account_id FROM accounts WHERE account_number = '10010003'),
        NULL,
        'SEED-OPEN-3',
        'CREDIT',
        'BRANCH',
        6800,
        6800,
        'Opening balance'
    ),
    (
        (SELECT account_id FROM accounts WHERE account_number = '10010004'),
        NULL,
        'SEED-OPEN-4',
        'CREDIT',
        'BRANCH',
        54000,
        54000,
        'Opening balance'
    ),
    (
        (SELECT account_id FROM accounts WHERE account_number = '99990001'),
        NULL,
        'SEED-OPEN-5',
        'CREDIT',
        'BRANCH',
        1500,
        1500,
        'Concurrency demo opening balance'
    );

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
VALUES
    (
        (SELECT customer_id FROM customers WHERE phone = '9000000001'),
        (SELECT account_id FROM accounts WHERE account_number = '10010001'),
        (SELECT branch_id FROM branches WHERE branch_code = 'BR001'),
        'HOME',
        300000,
        10.5,
        12,
        26455.71,
        date('now', '-60 day'),
        'ACTIVE'
    ),
    (
        (SELECT customer_id FROM customers WHERE phone = '9000000002'),
        (SELECT account_id FROM accounts WHERE account_number = '10010002'),
        (SELECT branch_id FROM branches WHERE branch_code = 'BR002'),
        'VEHICLE',
        180000,
        9.2,
        10,
        18805.41,
        date('now', '-35 day'),
        'ACTIVE'
    );

INSERT INTO emis (loan_id, installment_no, due_date, amount, status, paid_on)
VALUES
    (
        1,
        1,
        date('now', '-30 day'),
        26455.71,
        'PAID',
        date('now', '-30 day')
    ),
    (
        1,
        2,
        date('now', '-3 day'),
        26455.71,
        'OVERDUE',
        NULL
    ),
    (
        1,
        3,
        date('now', '+27 day'),
        26455.71,
        'PENDING',
        NULL
    ),
    (
        2,
        1,
        date('now', '-5 day'),
        18805.41,
        'PAID',
        date('now', '-5 day')
    ),
    (
        2,
        2,
        date('now', '+25 day'),
        18805.41,
        'PENDING',
        NULL
    );

INSERT INTO fixed_deposits (
    customer_id,
    linked_account_id,
    principal_amount,
    interest_rate,
    start_date,
    maturity_date,
    status
)
VALUES
    (
        (SELECT customer_id FROM customers WHERE phone = '9000000004'),
        (SELECT account_id FROM accounts WHERE account_number = '10010004'),
        150000,
        7.1,
        date('now', '-90 day'),
        date('now', '+275 day'),
        'ACTIVE'
    ),
    (
        (SELECT customer_id FROM customers WHERE phone = '9000000002'),
        (SELECT account_id FROM accounts WHERE account_number = '10010002'),
        85000,
        6.9,
        date('now', '-40 day'),
        date('now', '+325 day'),
        'ACTIVE'
    );

INSERT INTO atm_logs (account_id, atm_code, operation_type, amount, status)
VALUES
    (
        (SELECT account_id FROM accounts WHERE account_number = '10010001'),
        'ATM-PUNE-01',
        'WITHDRAWAL',
        5000,
        'SUCCESS'
    ),
    (
        (SELECT account_id FROM accounts WHERE account_number = '10010003'),
        'ATM-NAS-04',
        'BALANCE_INQUIRY',
        0,
        'SUCCESS'
    ),
    (
        (SELECT account_id FROM accounts WHERE account_number = '10010002'),
        'ATM-MUM-08',
        'WITHDRAWAL',
        70000,
        'FAILED'
    );
