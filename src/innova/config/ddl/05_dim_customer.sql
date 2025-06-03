CREATE TABLE dwh_innova.dim_customer (
    id_customer             INT IDENTITY(1,1) PRIMARY KEY,
    source_customer_id      VARCHAR(50) UNIQUE,
    customer_name           VARCHAR(150),
    id_segment              INT,
    id_location             INT,
    created_at              DATETIME DEFAULT GETDATE(),
    updated_at              DATETIME NULL,
    deleted_at              DATETIME NULL,
    CONSTRAINT fk_customer_segment
        FOREIGN KEY (id_segment)
        REFERENCES dwh_innova.dim_segment(id_segment),
    CONSTRAINT fk_customer_location
        FOREIGN KEY (id_location)
        REFERENCES dwh_innova.dim_location(id_location)
);