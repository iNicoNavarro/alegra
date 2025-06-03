CREATE TABLE dwh_innova.dim_date (
    date_id           INT PRIMARY KEY,        -- yyyymmdd
    full_date         DATE,
    day_of_month      TINYINT,
    month             TINYINT,
    month_name        VARCHAR(15),
    quarter           TINYINT,
    year              SMALLINT,
    week_of_year      TINYINT,
    is_weekend        BIT
);