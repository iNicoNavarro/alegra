CREATE TABLE dwh_innova.dim_segment (
    id_segment      INT         IDENTITY(1,1) PRIMARY KEY,
    segment_name    VARCHAR(50) NOT NULL UNIQUE,   -- Business Key
    created_at      DATETIME    NOT NULL DEFAULT GETDATE(),
    updated_at      DATETIME    NULL,
    deleted_at      DATETIME    NULL
);