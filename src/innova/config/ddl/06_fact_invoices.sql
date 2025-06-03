CREATE TABLE dwh_innova.fact_invoices (
    id_fact           BIGINT IDENTITY(1,1) PRIMARY KEY,
    id_customer       INT NOT NULL,
    id_product        INT NOT NULL,
    id_date           INT NOT NULL,           -- FK a dim_date
    quantity          INT,
    total_amount      DECIMAL(14,2),
    created_at        DATETIME DEFAULT GETDATE(),
    updated_at        DATETIME NULL,
    deleted_at        DATETIME NULL,
    CONSTRAINT fk_fact_customer 
        FOREIGN KEY (id_customer) REFERENCES dwh_innova.dim_customer(id_customer),
    CONSTRAINT fk_fact_product  
        FOREIGN KEY (id_product)  REFERENCES dwh_innova.dim_product(id_product),
    CONSTRAINT fk_fact_date     
        FOREIGN KEY (id_date)     REFERENCES dwh_innova.dim_date(date_id)
);