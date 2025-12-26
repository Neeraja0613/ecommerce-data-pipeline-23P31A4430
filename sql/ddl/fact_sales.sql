CREATE TABLE IF NOT EXISTS warehouse.fact_sales (
    sales_key BIGSERIAL PRIMARY KEY,
    date_key INT NOT NULL REFERENCES warehouse.dim_date(date_key),
    customer_key INT NOT NULL REFERENCES warehouse.dim_customers(customer_key),
    product_key INT NOT NULL REFERENCES warehouse.dim_products(product_key),
    payment_method_key INT NOT NULL REFERENCES warehouse.dim_payment_method(payment_method_key),
    transaction_id VARCHAR(20),
    quantity INT,
    unit_price DECIMAL(12,2),
    discount_amount DECIMAL(12,2),
    line_total DECIMAL(12,2),
    profit DECIMAL(12,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
