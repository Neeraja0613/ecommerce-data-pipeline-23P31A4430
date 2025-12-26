CREATE TABLE IF NOT EXISTS warehouse.dim_payment_method (
    payment_method_key SERIAL PRIMARY KEY,
    payment_method_name VARCHAR(50) UNIQUE NOT NULL,
    payment_type VARCHAR(20) NOT NULL
);
