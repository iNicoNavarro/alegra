CREATE TABLE dwh_innova.dim_product_category (
    id_category       INT IDENTITY(1,1) PRIMARY KEY,
    category_name     VARCHAR(100),
    created_at        DATETIME DEFAULT GETDATE(),
    updated_at        DATETIME NULL,
    deleted_at        DATETIME NULL
);
