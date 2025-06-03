CREATE TABLE dwh_innova.dim_location (
    id_location    INT         IDENTITY(1,1) PRIMARY KEY, 
    city           VARCHAR(50) NOT NULL,
    created_at     DATETIME    NOT NULL DEFAULT GETDATE(),
    updated_at     DATETIME    NULL,
    deleted_at     DATETIME    NULL,
    CONSTRAINT uq_dim_location_business_key 
        UNIQUE(city)   
);
