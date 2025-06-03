CREATE TABLE dwh_innova.dim_product (
    id_product         INT         IDENTITY(1,1) PRIMARY KEY,      -- Surrogate Key
    source_product_id  INT         NOT NULL UNIQUE,               -- Business Key (viene del CSV)
    product_name       VARCHAR(150) NOT NULL,
    id_category        INT         NOT NULL,                     -- FK a dim_product_category(id_category)
    price              DECIMAL(18,14) NOT NULL,
    created_at         DATETIME    NOT NULL DEFAULT GETDATE(),
    updated_at         DATETIME    NULL,
    deleted_at         DATETIME    NULL,
    CONSTRAINT fk_prod_category
        FOREIGN KEY (id_category)
        REFERENCES dwh_innova.dim_product_category(id_category)
);