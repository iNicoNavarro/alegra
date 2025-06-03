WITH ranked_products AS (
  SELECT
    d.[year],
    d.quarter,
    p.product_name,
    SUM(f.quantity) AS total_quantity,
    ROW_NUMBER() 
      OVER (
        PARTITION BY d.[year], d.quarter 
        ORDER BY SUM(f.quantity) DESC
      ) AS rn
  FROM dwh_innova.fact_invoices AS f
    JOIN dwh_innova.dim_date AS d
      ON f.id_date = d.date_id
    JOIN dwh_innova.dim_product AS p
      ON f.id_product = p.id_product
  GROUP BY
    d.[year],
    d.quarter,
    p.product_name
)
SELECT
  [year],
  quarter,
  product_name,
  total_quantity
FROM ranked_products
WHERE rn = 1;
